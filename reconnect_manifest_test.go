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

package nats

import (
	"bufio"
	"fmt"
	"net"
	"strings"
	"sync"
	"testing"
	"time"
)

// fakeNATSSession is a single client connection accepted by fakeNATSServer.
type fakeNATSSession struct {
	id   int
	conn net.Conn

	mu   sync.Mutex
	subs []string
}

func (s *fakeNATSSession) recordSub(line string) {
	s.mu.Lock()
	s.subs = append(s.subs, line)
	s.mu.Unlock()
}

func (s *fakeNATSSession) snapshotSubs() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.subs...)
}

// waitForSubs polls the session until at least n SUB lines have been read
// from the socket or the timeout elapses.
func (s *fakeNATSSession) waitForSubs(t *testing.T, n int, timeout time.Duration) []string {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		got := s.snapshotSubs()
		if len(got) >= n {
			return got
		}
		time.Sleep(2 * time.Millisecond)
	}
	got := s.snapshotSubs()
	t.Fatalf("fake server session %d: expected %d SUBs within %v, got %d: %v", s.id, n, timeout, len(got), got)
	return nil
}

// fakeNATSServer is a minimal, fully local NATS server fixture. It performs
// no authentication regardless of credentials, answers PING with PONG, and
// records every SUB protocol line received on each accepted connection. The
// test can forcibly close the active client connection to simulate a network
// break while the listener keeps accepting the reconnect.
type fakeNATSServer struct {
	t        *testing.T
	listener net.Listener

	mu       sync.Mutex
	nextID   int
	sessions map[int]*fakeNATSSession
	accepts  chan int // signaled for every accepted connection
	closed   bool
}

func startFakeNATSServer(t *testing.T) *fakeNATSServer {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("fake server listen: %v", err)
	}
	s := &fakeNATSServer{
		t:        t,
		listener: l,
		sessions: make(map[int]*fakeNATSSession),
		accepts:  make(chan int, 16),
	}
	go s.acceptLoop()
	return s
}

func (s *fakeNATSServer) url() string {
	return fmt.Sprintf("nats://127.0.0.1:%d", s.listener.Addr().(*net.TCPAddr).Port)
}

func (s *fakeNATSServer) acceptLoop() {
	for {
		c, err := s.listener.Accept()
		if err != nil {
			return
		}
		s.mu.Lock()
		if s.closed {
			s.mu.Unlock()
			c.Close()
			return
		}
		s.nextID++
		id := s.nextID
		sess := &fakeNATSSession{id: id, conn: c}
		s.sessions[id] = sess
		s.mu.Unlock()
		s.accepts <- id
		go s.handle(sess)
	}
}

func (s *fakeNATSServer) handle(sess *fakeNATSSession) {
	defer sess.conn.Close()
	// Minimal INFO: no nonce so the client does not attempt sign-in.
	if _, err := sess.conn.Write([]byte("INFO {\"server_id\":\"fake-server\"}\r\n")); err != nil {
		return
	}
	br := bufio.NewReaderSize(sess.conn, 64*1024)
	for {
		line, err := br.ReadString('\n')
		if err != nil {
			return
		}
		line = strings.TrimRight(line, "\r\n")
		switch {
		case strings.HasPrefix(line, "CONNECT"):
			// No-op: the fixture accepts every identity and credential.
		case strings.HasPrefix(line, "PING"):
			if _, err := sess.conn.Write([]byte(_PONG_OP_ + _CRLF_)); err != nil {
				return
			}
		case strings.HasPrefix(line, "SUB"):
			sess.recordSub(line)
		default:
			// PUB/HPUB/UNSUB and empty lines are ignored by this fixture.
		}
	}
}

// waitForAccept blocks until a new connection is accepted and returns it.
func (s *fakeNATSServer) waitForAccept(timeout time.Duration) *fakeNATSSession {
	select {
	case id := <-s.accepts:
		s.mu.Lock()
		defer s.mu.Unlock()
		return s.sessions[id]
	case <-time.After(timeout):
		s.t.Fatalf("fake server: did not accept a connection within %v", timeout)
		return nil
	}
}

func (s *fakeNATSServer) disconnectCurrent() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for id, sess := range s.sessions {
		sess.conn.Close()
		delete(s.sessions, id)
	}
}

func (s *fakeNATSServer) stop() {
	s.mu.Lock()
	s.closed = true
	s.mu.Unlock()
	s.listener.Close()
}

func newFakeReconnectConn(t *testing.T, s *fakeNATSServer) (*Conn, chan bool, chan bool) {
	t.Helper()
	dch := make(chan bool, 16)
	rch := make(chan bool, 16)
	nc, err := Connect(s.url(),
		Name("reconnect-manifest-test"),
		// Credentials are passed on purpose to assert they never surface
		// through the recovery manifest.
		UserInfo("manifest-user", "manifest-secret"),
		Token("manifest-token"),
		MaxReconnects(-1),
		ReconnectWait(15*time.Millisecond),
		ReconnectJitter(0, 0),
		DisconnectErrHandler(func(*Conn, error) {
			select {
			case dch <- true:
			default:
			}
		}),
		ReconnectHandler(func(*Conn) {
			select {
			case rch <- true:
			default:
			}
		}),
	)
	if err != nil {
		t.Fatalf("connect to fake server: %v", err)
	}
	return nc, dch, rch
}

// TestReconnectManifestRecovery is the happy path: subscriptions present at
// disconnect time move from Pending to Recovered, in stable sid order, and
// are actually resent to the new connection.
func TestReconnectManifestRecovery(t *testing.T) {
	srv := startFakeNATSServer(t)
	defer srv.stop()

	nc, dch, rch := newFakeReconnectConn(t, srv)
	defer nc.Close()

	sess1 := srv.waitForAccept(2 * time.Second)

	subjects := []string{"charlie", "alpha", "bravo"}
	subs := make([]*Subscription, 3)
	for i, subj := range subjects {
		sub, err := nc.Subscribe(subj, func(*Msg) {})
		if err != nil {
			t.Fatalf("subscribe %s: %v", subj, err)
		}
		subs[i] = sub
	}
	if err := nc.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}

	if got := sess1.snapshotSubs(); len(got) != 3 {
		t.Fatalf("first connection: expected 3 SUBs, got %v", got)
	}

	// While connected the manifest must be empty.
	if state := nc.SubscriptionReconnectState(); len(state.Pending) != 0 || len(state.Recovered) != 0 {
		t.Fatalf("expected empty manifest while connected, got %+v", state)
	}

	// Break the connection.
	srv.disconnectCurrent()
	if err := WaitTime(dch, 2*time.Second); err != nil {
		t.Fatal("missing disconnect callback")
	}

	if !nc.IsReconnecting() {
		t.Fatal("expected IsReconnecting() to be true")
	}

	// During the outage Pending lists the sids, sorted, and Recovered is
	// from no previous cycle.
	pendingIDs := []int64{subs[0].ID(), subs[1].ID(), subs[2].ID()}
	checkState := func() {
		state := nc.SubscriptionReconnectState()
		if len(state.Pending) != 3 {
			t.Fatalf("expected 3 pending, got %v", state.Pending)
		}
		for i, want := range pendingIDs {
			if state.Pending[i] != want {
				t.Fatalf("pending order mismatch at %d: got %d want %d (full %v)", i, state.Pending[i], want, state.Pending)
			}
		}
		if len(state.Recovered) != 0 {
			t.Fatalf("expected no recovered mid-outage, got %v", state.Recovered)
		}
	}
	checkState()

	// The returned slices must be copies: mutating them cannot affect the
	// connection internal manifest.
	state := nc.SubscriptionReconnectState()
	state.Pending[0] = -999
	state.Recovered = append(state.Recovered, 12345)
	checkState()

	// Allow the reconnect.
	sess2 := srv.waitForAccept(2 * time.Second)
	if err := WaitTime(rch, 2*time.Second); err != nil {
		t.Fatal("missing reconnect callback")
	}

	if nc.IsReconnecting() {
		t.Fatal("expected connection to be reconnected")
	}

	state = nc.SubscriptionReconnectState()
	if len(state.Pending) != 0 {
		t.Fatalf("expected no pending after reconnect, got %v", state.Pending)
	}
	if len(state.Recovered) != 3 {
		t.Fatalf("expected 3 recovered, got %v", state.Recovered)
	}
	for i, want := range pendingIDs {
		if state.Recovered[i] != want {
			t.Fatalf("recovered order mismatch at %d: got %d want %d (full %v)", i, state.Recovered[i], want, state.Recovered)
		}
	}

	// The resend order on the wire must match the stable sid order.
	gotSubs := sess2.waitForSubs(t, 3, 2*time.Second)
	for i, subj := range subjects {
		want := strings.TrimRight(fmt.Sprintf(subProto, subj, _EMPTY_, pendingIDs[i]), _CRLF_)
		if gotSubs[i] != want {
			t.Fatalf("SUB %d mismatch:\n got: %q\nwant: %q\norder: %v", i, gotSubs[i], want, gotSubs)
		}
	}

	// Manifest reports only numeric IDs; no subject or credential leakage.
	rendered := fmt.Sprintf("%v", state)
	for _, secret := range []string{"manifest-user", "manifest-secret", "manifest-token", "charlie", "alpha", "bravo"} {
		if strings.Contains(rendered, secret) {
			t.Fatalf("manifest leaked %q: %s", secret, rendered)
		}
	}
}

// TestReconnectManifestUnsubscribeDuringOutage verifies that a subscription
// cancelled while disconnected is removed from Pending and never resent or
// marked recovered.
func TestReconnectManifestUnsubscribeDuringOutage(t *testing.T) {
	srv := startFakeNATSServer(t)
	defer srv.stop()

	nc, dch, rch := newFakeReconnectConn(t, srv)
	defer nc.Close()

	sess1 := srv.waitForAccept(2 * time.Second)

	keepSub, err := nc.Subscribe("keep", func(*Msg) {})
	if err != nil {
		t.Fatalf("subscribe keep: %v", err)
	}
	goneSub, err := nc.Subscribe("gone", func(*Msg) {})
	if err != nil {
		t.Fatalf("subscribe gone: %v", err)
	}
	if err := nc.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	if got := sess1.snapshotSubs(); len(got) != 2 {
		t.Fatalf("expected 2 initial SUBs, got %v", got)
	}

	// Break the connection and cancel one subscription during the outage.
	srv.disconnectCurrent()
	if err := WaitTime(dch, 2*time.Second); err != nil {
		t.Fatal("missing disconnect callback")
	}

	if err := goneSub.Unsubscribe(); err != nil {
		t.Fatalf("unsubscribe while disconnected: %v", err)
	}

	state := nc.SubscriptionReconnectState()
	if len(state.Pending) != 1 || state.Pending[0] != keepSub.ID() {
		t.Fatalf("pending should only contain keep sub %d, got %v", keepSub.ID(), state.Pending)
	}
	if goneSub.IsValid() {
		t.Fatal("cancelled subscription should no longer be valid")
	}

	// Reconnect.
	sess2 := srv.waitForAccept(2 * time.Second)
	if err := WaitTime(rch, 2*time.Second); err != nil {
		t.Fatal("missing reconnect callback")
	}

	state = nc.SubscriptionReconnectState()
	if len(state.Pending) != 0 {
		t.Fatalf("expected empty pending, got %v", state.Pending)
	}
	if len(state.Recovered) != 1 || state.Recovered[0] != keepSub.ID() {
		t.Fatalf("expected only keep sub %d recovered, got %v", keepSub.ID(), state.Recovered)
	}

	// Wire check: only "keep" is resent; "gone" must never reappear.
	gotSubs := sess2.waitForSubs(t, 1, 2*time.Second)
	if want := strings.TrimRight(fmt.Sprintf(subProto, "keep", _EMPTY_, keepSub.ID()), _CRLF_); gotSubs[0] != want {
		t.Fatalf("resent SUB mismatch:\n got: %q\nwant: %q", gotSubs[0], want)
	}
	for _, line := range gotSubs {
		if strings.Contains(line, "gone") {
			t.Fatalf("cancelled subscription was resent: %v", gotSubs)
		}
	}
}

// TestReconnectManifestSubscribeDuringOutage verifies that subscriptions
// created while disconnected join the pending list and are established on
// the new connection.
func TestReconnectManifestSubscribeDuringOutage(t *testing.T) {
	srv := startFakeNATSServer(t)
	defer srv.stop()

	nc, dch, rch := newFakeReconnectConn(t, srv)
	defer nc.Close()

	srv.waitForAccept(2 * time.Second)

	oldSub, err := nc.Subscribe("old", func(*Msg) {})
	if err != nil {
		t.Fatalf("subscribe old: %v", err)
	}
	if err := nc.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}

	srv.disconnectCurrent()
	if err := WaitTime(dch, 2*time.Second); err != nil {
		t.Fatal("missing disconnect callback")
	}

	newSub, err := nc.Subscribe("new", func(*Msg) {})
	if err != nil {
		t.Fatalf("subscribe while disconnected: %v", err)
	}

	state := nc.SubscriptionReconnectState()
	if len(state.Pending) != 2 {
		t.Fatalf("expected 2 pending, got %v", state.Pending)
	}
	if state.Pending[0] != oldSub.ID() || state.Pending[1] != newSub.ID() {
		t.Fatalf("pending sids out of order: %v (old=%d new=%d)", state.Pending, oldSub.ID(), newSub.ID())
	}

	sess2 := srv.waitForAccept(2 * time.Second)
	if err := WaitTime(rch, 2*time.Second); err != nil {
		t.Fatal("missing reconnect callback")
	}

	state = nc.SubscriptionReconnectState()
	if len(state.Recovered) != 2 || state.Recovered[0] != oldSub.ID() || state.Recovered[1] != newSub.ID() {
		t.Fatalf("unexpected recovered list: %v", state.Recovered)
	}

	gotSubs := sess2.waitForSubs(t, 2, 2*time.Second)
	if want := strings.TrimRight(fmt.Sprintf(subProto, "old", _EMPTY_, oldSub.ID()), _CRLF_); gotSubs[0] != want {
		t.Fatalf("first resent SUB mismatch: got %q want %q", gotSubs[0], want)
	}
	if want := strings.TrimRight(fmt.Sprintf(subProto, "new", _EMPTY_, newSub.ID()), _CRLF_); gotSubs[1] != want {
		t.Fatalf("second resent SUB mismatch: got %q want %q", gotSubs[1], want)
	}
}

// TestReconnectManifestMultipleCycles verifies the manifest resets on each
// new outage and reports only the most recent cycle recovered sids.
func TestReconnectManifestMultipleCycles(t *testing.T) {
	srv := startFakeNATSServer(t)
	defer srv.stop()

	nc, dch, rch := newFakeReconnectConn(t, srv)
	defer nc.Close()

	sess1 := srv.waitForAccept(2 * time.Second)

	sub1, err := nc.Subscribe("one", func(*Msg) {})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	if err := nc.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	if got := sess1.snapshotSubs(); len(got) != 1 {
		t.Fatalf("expected 1 initial SUB, got %v", got)
	}

	// First cycle.
	srv.disconnectCurrent()
	if err := WaitTime(dch, 2*time.Second); err != nil {
		t.Fatal("missing disconnect callback (1)")
	}
	srv.waitForAccept(2 * time.Second)
	if err := WaitTime(rch, 2*time.Second); err != nil {
		t.Fatal("missing reconnect callback (1)")
	}
	if state := nc.SubscriptionReconnectState(); len(state.Recovered) != 1 || state.Recovered[0] != sub1.ID() {
		t.Fatalf("cycle 1 recovered mismatch: %+v", state)
	}

	// Second cycle: add a subscription, cancel the first one.
	srv.disconnectCurrent()
	if err := WaitTime(dch, 2*time.Second); err != nil {
		t.Fatal("missing disconnect callback (2)")
	}

	if state := nc.SubscriptionReconnectState(); len(state.Pending) != 1 || state.Pending[0] != sub1.ID() {
		t.Fatalf("cycle 2 initial pending mismatch: %+v", state)
	}
	if len(nc.SubscriptionReconnectState().Recovered) != 0 {
		t.Fatal("recovered list must be cleared when a new cycle starts")
	}

	if err := sub1.Unsubscribe(); err != nil {
		t.Fatalf("unsubscribe during cycle 2: %v", err)
	}
	sub2, err := nc.Subscribe("two", func(*Msg) {})
	if err != nil {
		t.Fatalf("subscribe during cycle 2: %v", err)
	}

	srv.waitForAccept(2 * time.Second)
	if err := WaitTime(rch, 2*time.Second); err != nil {
		t.Fatal("missing reconnect callback (2)")
	}

	state := nc.SubscriptionReconnectState()
	if len(state.Pending) != 0 {
		t.Fatalf("expected empty pending, got %v", state.Pending)
	}
	if len(state.Recovered) != 1 || state.Recovered[0] != sub2.ID() {
		t.Fatalf("cycle 2 recovered should only contain sub2 %d, got %v", sub2.ID(), state.Recovered)
	}
}

// TestReconnectManifestExhaustedClearsManifest verifies the failure path:
// when reconnection is given up entirely, the manifest is cleared rather
// than reporting stale pending entries.
func TestReconnectManifestExhaustedClearsManifest(t *testing.T) {
	srv := startFakeNATSServer(t)

	dch := make(chan bool, 16)
	closed := make(chan bool, 1)
	nc, err := Connect(srv.url(),
		MaxReconnects(1),
		ReconnectWait(5*time.Millisecond),
		ReconnectJitter(0, 0),
		DisconnectErrHandler(func(*Conn, error) {
			select {
			case dch <- true:
			default:
			}
		}),
		ClosedHandler(func(*Conn) { closed <- true }),
	)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}

	sess := srv.waitForAccept(2 * time.Second)
	sub, err := nc.Subscribe("doomed", func(*Msg) {})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	if err := nc.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}

	// Stop the whole server so reconnect attempts fail and the client
	// gives up after MaxReconnects.
	srv.stop()
	sess.conn.Close()
	if err := WaitTime(dch, 2*time.Second); err != nil {
		t.Fatal("missing disconnect callback")
	}

	// While attempting, the sid is pending.
	deadline := time.Now().Add(2 * time.Second)
	sawPending := false
	for time.Now().Before(deadline) {
		if state := nc.SubscriptionReconnectState(); len(state.Pending) == 1 && state.Pending[0] == sub.ID() {
			sawPending = true
			break
		}
		time.Sleep(2 * time.Millisecond)
	}
	if !sawPending {
		t.Fatal("never observed the doomed subscription in the pending list")
	}

	if err := WaitTime(closed, 3*time.Second); err != nil {
		t.Fatal("connection should have been closed after exhausting reconnects")
	}

	state := nc.SubscriptionReconnectState()
	if len(state.Pending) != 0 || len(state.Recovered) != 0 {
		t.Fatalf("manifest must be cleared after giving up, got %+v", state)
	}
}

// TestSubscriptionIDAccessors verifies the read-only sid accessor used to
// correlate subscriptions with manifest entries.
func TestSubscriptionIDAccessors(t *testing.T) {
	srv := startFakeNATSServer(t)
	defer srv.stop()

	nc, _, _ := newFakeReconnectConn(t, srv)
	defer nc.Close()
	srv.waitForAccept(2 * time.Second)

	var nilSub *Subscription
	if got := nilSub.ID(); got != -1 {
		t.Fatalf("nil subscription ID: got %d, want -1", got)
	}

	s1, err := nc.Subscribe("s1", func(*Msg) {})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	s2, err := nc.SubscribeSync("s2")
	if err != nil {
		t.Fatalf("subscribe sync: %v", err)
	}
	if s1.ID() <= 0 || s2.ID() != s1.ID()+1 {
		t.Fatalf("unexpected sequential sids: %d, %d", s1.ID(), s2.ID())
	}
	// The sid must remain stable across repeated reads, including while
	// the connection is reconnecting.
	first := s1.ID()
	srv.disconnectCurrent()
	if id := s1.ID(); id != first {
		t.Fatalf("sid changed during reconnect: got %d want %d", id, first)
	}
	srv.waitForAccept(2 * time.Second)
	if id := s1.ID(); id != first {
		t.Fatalf("sid changed after reconnect: got %d want %d", id, first)
	}
}
