// Copyright 2026 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package jetstream

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/internal/syncx"
)

// fakePullNATSServer is a minimal NATS protocol server used to
// deterministically drive a pull subscription. It only implements the subset
// of the NATS wire protocol exercised by the jetstream pull code:
//
//   - CONNECT/PING/PUB/SUB/UNSUB on the client side
//   - MSG/HMSG on the server side
//
// The server publishes raw JetStream stream messages (MSG) and
// idle-heartbeat status frames (HMSG with "Status: 100") directly to the
// inbox used by the client pull request. No JetStream server, Docker or
// tester service is required.
type fakePullNATSServer struct {
	listener net.Listener

	mu     sync.Mutex
	closed bool
	conns  map[*fakePullConn]struct{}
}

type fakePullConn struct {
	server *fakePullNATSServer
	conn   net.Conn
	wmu    sync.Mutex

	subsMu sync.Mutex
	subs   map[string]string // subject -> sid
	// replyInbox is the JS pull request reply inbox last used by the client.
	replyInbox string
	// seenInboxes are delivery inboxes already returned by waitForInbox, so
	// repeated calls wait for the next pull request (same connection or a
	// new one).
	seenInboxes map[string]struct{}
	// lastSubInbox is the newest inbox the client subscribed to.
	lastSubInbox string
	// pulls tracks active pull requests by delivery inbox, emulating the
	// server-side pull state: the wall-clock expiry, the requested batch and
	// how many messages of the batch have already been delivered.
	pulls map[string]*fakePull
	// pullHeartbeat is the heartbeat interval announced by the client
	// (from the pull request JSON). The fixture sends a 100 control frame on
	// this cadence for every active pull, mirroring a JetStream server.
	pullHeartbeat time.Duration
	// heartbeatCount counts 100 control frames actually sent.
	heartbeatCount atomic.Int64
	// nextHeartbeat is the scheduled time of the next steady-cadence
	// heartbeat; it survives pull expirations and re-issues.
	nextHeartbeat time.Time
	// stopReaper terminates background goroutines for the connection.
	stopReaper chan struct{}
}

// fakePull emulates a single server-side pull request on a delivery inbox.
type fakePull struct {
	expiresAt time.Time
	batch     int
	delivered int
}

// heartbeatCounter is a test helper handle for reading heartbeat counts.
type heartbeatCounter struct {
	conn *fakePullConn
}

func (c *fakePullConn) startHeartbeatCounter() *heartbeatCounter {
	return &heartbeatCounter{conn: c}
}

func (h *heartbeatCounter) count() int { return int(h.conn.heartbeatCount.Load()) }

func (h *heartbeatCounter) stop() {}

func startFakePullNATSServer(t *testing.T) *fakePullNATSServer {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("start fake nats server: %v", err)
	}
	s := &fakePullNATSServer{
		listener: ln,
		conns:    make(map[*fakePullConn]struct{}),
	}
	go s.acceptLoop()
	t.Cleanup(s.shutdown)
	return s
}

func (s *fakePullNATSServer) url() string {
	return "nats://" + s.listener.Addr().String()
}

func (s *fakePullNATSServer) acceptLoop() {
	for {
		c, err := s.listener.Accept()
		if err != nil {
			return
		}
		fc := &fakePullConn{
			server:      s,
			conn:        c,
			subs:        make(map[string]string),
			seenInboxes: make(map[string]struct{}),
			pulls:       make(map[string]*fakePull),
			stopReaper:  make(chan struct{}),
		}
		s.mu.Lock()
		if s.closed {
			s.mu.Unlock()
			c.Close()
			return
		}
		s.conns[fc] = struct{}{}
		s.mu.Unlock()
		// A NATS server starts by sending an INFO frame.
		fc.write("INFO " + `{"server_id":"fake","server_name":"fake","version":"2.0.0","go":"go1.26","host":"127.0.0.1","port":0,"headers":true,"max_payload":1048576,"proto":1}` + "\r\n")
		fc.runHeartbeats()
		go fc.readLoop()
	}
}

func (s *fakePullNATSServer) shutdown() {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	s.closed = true
	s.listener.Close()
	for c := range s.conns {
		c.conn.Close()
	}
	s.mu.Unlock()
}

func (c *fakePullConn) readLoop() {
	defer func() {
		close(c.stopReaper)
		c.server.mu.Lock()
		delete(c.server.conns, c)
		c.server.mu.Unlock()
		c.conn.Close()
	}()
	reader := bufio.NewReaderSize(c.conn, 64*1024)
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			return
		}
		line = strings.TrimRight(line, "\r\n")
		if line == "" {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) == 0 {
			continue
		}
		switch fields[0] {
		case "CONNECT":
			// No acknowledgement required for CONNECT.
		case "PING":
			c.write("PONG\r\n")
		case "SUB":
			// SUB <subject> [queue-group] <sid>
			if len(fields) < 3 {
				continue
			}
			subj := fields[1]
			sid := fields[len(fields)-1]
			c.subsMu.Lock()
			c.subs[subj] = sid
			if strings.Contains(subj, "INBOX") {
				c.lastSubInbox = subj
			}
			c.subsMu.Unlock()
		case "UNSUB":
			if len(fields) < 2 {
				continue
			}
			sid := fields[1]
			c.subsMu.Lock()
			for subj, s := range c.subs {
				if s == sid {
					delete(c.subs, subj)
					delete(c.pulls, subj)
				}
			}
			c.subsMu.Unlock()
		case "PUB", "HPUB":
			// PUB <subject> [reply-to] <#bytes>
			if len(fields) < 3 {
				continue
			}
			size, err := strconv.Atoi(fields[len(fields)-1])
			if err != nil {
				continue
			}
			payload := make([]byte, size)
			if _, err := io.ReadFull(reader, payload); err != nil {
				return
			}
			c.handlePub(fields, payload)
		}
	}
}

// pullRequestPayload is the JSON body of a CONSUMER.MSG.NEXT request.
type pullRequestPayload struct {
	ExpiresNS   int64 `json:"expires,omitempty"`
	Batch       int   `json:"batch,omitempty"`
	HeartbeatNS int64 `json:"idle_heartbeat,omitempty"`
}

func (c *fakePullConn) handlePub(fields []string, payload []byte) {
	// Pull requests are published to the JS API next subject with the
	// delivery inbox as reply-to.
	if len(fields) < 3 || !strings.Contains(fields[1], ".CONSUMER.MSG.NEXT.") {
		return
	}
	inbox := fields[2]
	var pr pullRequestPayload
	_ = json.Unmarshal(payload, &pr)
	c.subsMu.Lock()
	c.replyInbox = inbox
	c.lastSubInbox = inbox
	var expiresAt time.Time
	if pr.ExpiresNS > 0 {
		expiresAt = time.Now().Add(time.Duration(pr.ExpiresNS))
	}
	if pr.HeartbeatNS > 0 {
		c.pullHeartbeat = time.Duration(pr.HeartbeatNS)
	}
	batch := pr.Batch
	if batch <= 0 {
		batch = 1
	}
	pull := &fakePull{expiresAt: expiresAt, batch: batch}
	c.pulls[inbox] = pull
	c.subsMu.Unlock()
	c.servePull(inbox, pull)
}

// servePull emulates a JetStream server-side pull on inbox: it ends the pull
// with a 408 status when it expires without a fulfilled batch. The 408 reports
// the still-pending remainder of the batch (Nats-Pending-Messages) so the
// client can reconcile its accounting and issue a follow-up pull, exactly as
// a real server does. Idle heartbeats are sent continuously by the
// per-connection heartbeat loop (see runHeartbeats) rather than per-pull.
func (c *fakePullConn) servePull(inbox string, pull *fakePull) {
	if pull.expiresAt.IsZero() {
		return
	}
	go func() {
		t := time.NewTimer(time.Until(pull.expiresAt))
		defer t.Stop()
		select {
		case <-t.C:
		case <-c.stopReaper:
			return
		}
		c.subsMu.Lock()
		active, ok := c.pulls[inbox]
		if !ok || active != pull {
			c.subsMu.Unlock()
			return
		}
		msgsLeft := active.batch - active.delivered
		if msgsLeft < 0 {
			msgsLeft = 0
		}
		delete(c.pulls, inbox)
		c.subsMu.Unlock()
		c.publishStatusToInbox(inbox, fmt.Sprintf(
			"NATS/1.0 408 Request Timeout\r\nStatus: 408\r\nDescription: Auto-created pull timer expired\r\nNats-Pending-Messages: %d\r\nNats-Pending-Bytes: 0\r\n\r\n",
			msgsLeft))
	}()
}

// activePullInbox returns an inbox that has both an active pull and a live
// subscription, preferring the most recent one.
func (c *fakePullConn) activePullInbox() (string, string) {
	c.subsMu.Lock()
	defer c.subsMu.Unlock()
	if sid := c.subs[c.lastSubInbox]; sid != "" {
		if _, ok := c.pulls[c.lastSubInbox]; ok {
			return c.lastSubInbox, sid
		}
	}
	var inbox, sid string
	for subj := range c.pulls {
		if s := c.subs[subj]; s != "" && subj > inbox {
			inbox, sid = subj, s
		}
	}
	return inbox, sid
}

// runHeartbeats continuously sends 100 idle-heartbeat frames to an active
// pull inbox at the interval last announced by the client. This mirrors a
// JetStream server, which heartbeats steadily regardless of pull
// expirations and re-issues.
func (c *fakePullConn) runHeartbeats() {
	go func() {
		ticker := time.NewTicker(20 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-c.stopReaper:
				return
			case <-ticker.C:
				c.subsMu.Lock()
				interval := c.pullHeartbeat
				next := c.nextHeartbeat
				c.subsMu.Unlock()
				if interval == 0 {
					continue
				}
				now := time.Now()
				if now.Before(next) {
					continue
				}
				inbox, sid := c.activePullInbox()
				if inbox == "" {
					continue
				}
				if c.writeStatusFrame(inbox, sid,
					"NATS/1.0 100 Idle Heartbeat\r\nStatus: 100\r\nDescription: Idle Heartbeat\r\n\r\n") {
					c.heartbeatCount.Add(1)
					c.subsMu.Lock()
					if c.nextHeartbeat.IsZero() || now.After(c.nextHeartbeat) {
						c.nextHeartbeat = now.Add(interval)
					} else {
						c.nextHeartbeat = c.nextHeartbeat.Add(interval)
					}
					c.subsMu.Unlock()
				}
			}
		}
	}()
}

func (c *fakePullConn) pullActive(inbox string) bool {
	c.subsMu.Lock()
	defer c.subsMu.Unlock()
	_, ok := c.pulls[inbox]
	return ok
}

func (c *fakePullConn) publishStatusToInbox(inbox, headerBlock string) bool {
	c.subsMu.Lock()
	sid := c.subs[inbox]
	c.subsMu.Unlock()
	if sid == "" {
		return false
	}
	return c.writeStatusFrame(inbox, sid, headerBlock)
}

func (c *fakePullConn) write(frame string) bool {
	c.wmu.Lock()
	defer c.wmu.Unlock()
	if _, err := c.conn.Write([]byte(frame)); err != nil {
		return false
	}
	return true
}

func (c *fakePullConn) sidForInbox(inbox string) string {
	c.subsMu.Lock()
	defer c.subsMu.Unlock()
	return c.subs[inbox]
}

// latestInbox returns the most recently subscribed delivery inbox and its
// SID. Every pull request uses a freshly generated inbox, so frames must be
// sent to the newest one (older inboxes are unsubscribed as pulls complete).
func (c *fakePullConn) latestInbox() (string, string) {
	c.subsMu.Lock()
	defer c.subsMu.Unlock()
	inbox := c.lastSubInbox
	return inbox, c.subs[inbox]
}

// deliverToPull records delivery of one real message against the active pull
// on inbox. A partial batch leaves the pull alive (and still heartbeatable)
// until it expires; a fulfilled batch retires the pull server-side.
func (c *fakePullConn) deliverToPull(inbox string) {
	c.subsMu.Lock()
	defer c.subsMu.Unlock()
	if pull, ok := c.pulls[inbox]; ok {
		pull.delivered++
		if pull.batch > 0 && pull.delivered >= pull.batch {
			delete(c.pulls, inbox)
		}
	}
}

// publishMsg sends a raw JetStream stream message (MSG frame) to the client's
// newest pull request delivery inbox.
func (c *fakePullConn) publishMsg(data string) bool {
	inbox, sid := c.latestInbox()
	if inbox == "" || sid == "" {
		return false
	}
	frame := fmt.Sprintf("MSG %s %s %d\r\n%s\r\n", inbox, sid, len(data), data)
	if c.write(frame) {
		c.deliverToPull(inbox)
		return true
	}
	return false
}

// writeStatusFrame sends an HMSG status frame with the given header block
// and empty body. On the wire an HMSG is
//
//	HMSG <subject> <sid> [reply] <hdrLen> <totalLen>\r\n
//	<hdrLen bytes of headers><body>\r\n
//
// where hdrLen covers the header block including its terminating blank line
// and, for an empty-body message, totalLen == hdrLen.
func (c *fakePullConn) writeStatusFrame(inbox, sid, headerBlock string) bool {
	frame := fmt.Sprintf("HMSG %s %s %d %d\r\n%s\r\n", inbox, sid, len(headerBlock), len(headerBlock), headerBlock)
	return c.write(frame)
}

// publishHeartbeat sends an idle-heartbeat control frame (HMSG, Status:
// 100, empty body) to the client's newest pull request delivery inbox.
func (c *fakePullConn) publishHeartbeat() bool {
	inbox, sid := c.latestInbox()
	if inbox == "" || sid == "" {
		return false
	}
	hdr := "NATS/1.0 100 Idle Heartbeat\r\nStatus: 100\r\nDescription: Idle Heartbeat\r\n\r\n"
	return c.writeStatusFrame(inbox, sid, hdr)
}

// waitForInbox blocks until a fresh pull request delivery inbox is ready
// (PUB received and inbox subscribed) or the timeout elapses. Inboxes already
// returned by a previous call are skipped, so several consumers or sequential
// pulls can be awaited on the same fake server.
func (s *fakePullNATSServer) waitForInbox(t *testing.T, timeout time.Duration) *fakePullConn {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		s.mu.Lock()
		for c := range s.conns {
			c.subsMu.Lock()
			inbox := c.replyInbox
			sid := c.subs[inbox]
			_, seen := c.seenInboxes[inbox]
			c.subsMu.Unlock()
			if !seen && inbox != "" && sid != "" {
				c.subsMu.Lock()
				c.seenInboxes[inbox] = struct{}{}
				c.subsMu.Unlock()
				s.mu.Unlock()
				return c
			}
		}
		s.mu.Unlock()
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("fake server: client never issued a pull request")
	return nil
}

// newPullConsumerWithFakeServer connects to the fake server and assembles a
// pullConsumer without going through the JetStream API, so tests have full
// control over the frames the "server" sends.
func newPullConsumerWithFakeServer(t *testing.T, s *fakePullNATSServer) (*nats.Conn, *pullConsumer) {
	t.Helper()
	nc, err := nats.Connect(s.url(), nats.Name("pull-idle-deadline-test"))
	if err != nil {
		t.Fatalf("connect to fake server: %v", err)
	}
	t.Cleanup(nc.Close)

	js, err := New(nc)
	if err != nil {
		t.Fatalf("create jetstream: %v", err)
	}
	consumer := &pullConsumer{
		js:     js.(*jetStream),
		stream: "TEST",
		name:   "C",
		subs:   syncx.Map[string, *pullSubscription]{},
		info: &ConsumerInfo{
			Stream: "TEST",
			Name:   "C",
			Config: ConsumerConfig{},
		},
	}
	return nc, consumer
}

const (
	idleTestExpiry     = 1 * time.Second
	idleTestHeartbeat  = 500 * time.Millisecond
	idleTestDeadline   = 1500 * time.Millisecond
	idleTestPullMaxMsg = 16
)

func idleTestConsumeOpts() []PullConsumeOpt {
	return []PullConsumeOpt{
		PullExpiry(idleTestExpiry),
		PullHeartbeat(idleTestHeartbeat),
		PullMaxMessages(idleTestPullMaxMsg),
		PullIdleDeadline(idleTestDeadline),
	}
}

// TestPullIdleDeadline_Timeout verifies that when the fake server only sends
// idle heartbeats, the absolute idle deadline fires even though heartbeats
// keep arriving: heartbeats do not refresh the deadline.
func TestPullIdleDeadline_Timeout(t *testing.T) {
	srv := startFakePullNATSServer(t)
	_, consumer := newPullConsumerWithFakeServer(t, srv)

	errCh := make(chan error, 10)
	msgsCh := make(chan Msg, 10)
	cc, err := consumer.Consume(func(msg Msg) {
		msgsCh <- msg
	}, append(idleTestConsumeOpts(), ConsumeErrHandler(func(_ ConsumeContext, err error) {
		errCh <- err
	}))...)
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}
	defer cc.Stop()

	fc := srv.waitForInbox(t, 2*time.Second)

	// The fixture itself sends heartbeats on the cadence announced by the
	// pull request (500ms here) and re-arms them on every re-pull. Count
	// them to prove heartbeats keep arriving but never refresh the absolute
	// 1.5s idle deadline.
	heartbeats := fc.startHeartbeatCounter()
	defer heartbeats.stop()

	start := time.Now()
	select {
	case err := <-errCh:
		if !errors.Is(err, ErrConsumerIdleDeadline) {
			t.Fatalf("expected ErrConsumerIdleDeadline, got: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("idle deadline never fired")
	}
	elapsed := time.Since(start)

	if elapsed < idleTestDeadline-300*time.Millisecond {
		t.Fatalf("deadline fired too early: %v", elapsed)
	}
	if elapsed > idleTestDeadline+1500*time.Millisecond {
		t.Fatalf("deadline fired too late: %v", elapsed)
	}
	if hb := heartbeats.count(); hb < 2 {
		t.Fatalf("expected several heartbeats before the deadline, got %d", hb)
	}

	select {
	case m := <-msgsCh:
		t.Fatalf("unexpected message delivered: %q", m.Data())
	default:
	}

	select {
	case <-cc.Closed():
	case <-time.After(3 * time.Second):
		t.Fatalf("consume context was not closed after idle deadline")
	}
}

// TestPullIdleDeadline_RefreshedByMessage verifies that receiving a real
// message refreshes the absolute deadline while interleaved heartbeats do
// not, so a trickle of messages keeps the consumer alive; once messages
// stop, the deadline fires from the last refresh point.
func TestPullIdleDeadline_RefreshedByMessage(t *testing.T) {
	srv := startFakePullNATSServer(t)
	_, consumer := newPullConsumerWithFakeServer(t, srv)

	iter, err := consumer.Messages(
		PullExpiry(idleTestExpiry),
		PullHeartbeat(idleTestHeartbeat),
		PullMaxMessages(idleTestPullMaxMsg),
		PullIdleDeadline(idleTestDeadline),
	)
	if err != nil {
		t.Fatalf("Messages: %v", err)
	}
	defer iter.Stop()

	const msgCount = 5
	var received int

	// The first pull request for Messages() is issued from the first Next()
	// call (via checkPending), so start it before waiting for the inbox.
	firstMsgCh := make(chan Msg, 1)
	firstErrCh := make(chan error, 1)
	go func() {
		m, err := iter.Next(NextMaxWait(3 * time.Second))
		firstMsgCh <- m
		firstErrCh <- err
	}()

	fc := srv.waitForInbox(t, 2*time.Second)

	framesDone := make(chan struct{})
	go func() {
		defer close(framesDone)
		// The fixture already sends heartbeats on the 500ms cadence
		// announced by the pull request. Here we only send the real
		// messages, one every 900ms. The 1.5s deadline can only survive
		// across the ~4.5s span because every real message renews it while
		// heartbeats do not.
		for i := 0; i < msgCount; i++ {
			time.Sleep(900 * time.Millisecond)
			fc.publishMsg(fmt.Sprintf("msg-%d", i+1))
		}
	}()

	for received < msgCount {
		var m Msg
		var err error
		if received == 0 {
			m = <-firstMsgCh
			err = <-firstErrCh
		} else {
			m, err = iter.Next(NextMaxWait(3 * time.Second))
		}
		if err != nil {
			t.Fatalf("Next %d: %v (idle deadline likely not refreshed)", received+1, err)
		}
		received++
		expected := fmt.Sprintf("msg-%d", received)
		if string(m.Data()) != expected {
			t.Fatalf("message %d: expected %q, got %q", received, expected, m.Data())
		}
	}
	<-framesDone

	// No more frames: the deadline must fire measured from the last message.
	start := time.Now()
	if _, err := iter.Next(NextMaxWait(5 * time.Second)); !errors.Is(err, ErrConsumerIdleDeadline) {
		t.Fatalf("expected ErrConsumerIdleDeadline after stream went quiet, got: %v", err)
	}
	elapsed := time.Since(start)
	if elapsed < idleTestDeadline-300*time.Millisecond {
		t.Fatalf("deadline fired too early after last message: %v", elapsed)
	}
}

// TestPullIdleDeadline_CancelNoDispatch verifies that once the consumer is
// canceled, messages and heartbeats still in flight are not dispatched.
func TestPullIdleDeadline_CancelNoDispatch(t *testing.T) {
	srv := startFakePullNATSServer(t)
	_, consumer := newPullConsumerWithFakeServer(t, srv)

	errCh := make(chan error, 10)
	msgsCh := make(chan Msg, 10)
	cc, err := consumer.Consume(func(msg Msg) {
		msgsCh <- msg
	}, append(idleTestConsumeOpts(), ConsumeErrHandler(func(_ ConsumeContext, err error) {
		// The idle deadline may race an explicit Stop; only other errors
		// are unexpected here.
		if !errors.Is(err, ErrConsumerIdleDeadline) {
			errCh <- err
		}
	}))...)
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}

	fc := srv.waitForInbox(t, 2*time.Second)
	fc.publishMsg("before-cancel")
	select {
	case msg := <-msgsCh:
		if string(msg.Data()) != "before-cancel" {
			t.Fatalf("unexpected first message: %q", msg.Data())
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("first message was not delivered")
	}

	cc.Stop()
	select {
	case <-cc.Closed():
	case <-time.After(3 * time.Second):
		t.Fatalf("consume context did not close after Stop")
	}

	for i := 0; i < 5; i++ {
		fc.publishMsg(fmt.Sprintf("after-cancel-%d", i))
		fc.publishHeartbeat()
		time.Sleep(20 * time.Millisecond)
	}

	select {
	case m := <-msgsCh:
		t.Fatalf("message dispatched after cancel: %q", m.Data())
	case err := <-errCh:
		t.Fatalf("unexpected error after cancel: %v", err)
	case <-time.After(500 * time.Millisecond):
		// Nothing delivered after cancellation.
	}

	// A stopped Messages iterator reports ErrMsgIteratorClosed and also
	// ignores frames that arrive after unsubscribe. The first pull request
	// for Messages() is only sent from the first Next() call, so start
	// Next() before waiting for the pull to be issued.
	iter, err := consumer.Messages(
		PullExpiry(idleTestExpiry),
		PullHeartbeat(idleTestHeartbeat),
		PullMaxMessages(idleTestPullMaxMsg),
	)
	if err != nil {
		t.Fatalf("Messages: %v", err)
	}
	nextErrCh := make(chan error, 1)
	go func() {
		_, err := iter.Next(NextMaxWait(2 * time.Second))
		nextErrCh <- err
	}()
	fc2 := srv.waitForInbox(t, 2*time.Second)
	iter.Stop()
	select {
	case err := <-nextErrCh:
		if !errors.Is(err, ErrMsgIteratorClosed) {
			t.Fatalf("expected ErrMsgIteratorClosed after Stop, got: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatalf("Next did not return after Stop")
	}
	time.Sleep(100 * time.Millisecond)
	fc2.publishMsg("late-message")
	// A subsequent Next is closed immediately.
	if _, err := iter.Next(NextMaxWait(time.Second)); !errors.Is(err, ErrMsgIteratorClosed) {
		t.Fatalf("expected ErrMsgIteratorClosed after Stop, got: %v", err)
	}
}

// TestPullIdleDeadline_Options verifies option validation.
func TestPullIdleDeadline_Options(t *testing.T) {
	srv := startFakePullNATSServer(t)
	_, consumer := newPullConsumerWithFakeServer(t, srv)

	t.Run("zero and negative values rejected", func(t *testing.T) {
		for _, opt := range []PullConsumeOpt{PullIdleDeadline(0), PullIdleDeadline(-time.Second)} {
			if _, err := consumer.Consume(func(Msg) {},
				PullExpiry(time.Second),
				PullHeartbeat(500*time.Millisecond),
				opt,
			); err == nil {
				t.Fatalf("expected error for invalid idle deadline option")
			}
		}
	})

	t.Run("deadline must be greater than expiry", func(t *testing.T) {
		_, err := consumer.Consume(func(Msg) {},
			PullExpiry(10*time.Second),
			PullHeartbeat(time.Second),
			PullIdleDeadline(5*time.Second),
		)
		if err == nil {
			t.Fatalf("expected error when idle deadline is not greater than expiry")
		}
	})

	t.Run("valid option accepted", func(t *testing.T) {
		cc, err := consumer.Consume(func(Msg) {},
			PullExpiry(time.Second),
			PullHeartbeat(500*time.Millisecond),
			PullIdleDeadline(2*time.Second),
		)
		if err != nil {
			t.Fatalf("Consume: %v", err)
		}
		cc.Stop()
	})
}
