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
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

// protocolFixture is a mock NATS server that speaks the NATS wire protocol.
// It handles the connection handshake, JetStream API requests (stream/consumer
// creation), and pull requests. On each pull request it emits idle heartbeats
// at a fixed interval; the test can inject real messages at controlled times.
// This lets the test deterministically exercise the idle deadline without a
// real JetStream server.
type protocolFixture struct {
	t      *testing.T
	l      net.Listener
	addr   string
	mu     sync.Mutex
	subs   map[string]int64
	conn   net.Conn
	connMu sync.Mutex

	heartbeatInterval time.Duration

	pullCh chan *fixturePull
	done   chan struct{}
	wg     sync.WaitGroup

	latestReply string
}

type fixturePull struct {
	subject string
	reply   string
}

func newProtocolFixture(t *testing.T, heartbeatInterval time.Duration) *protocolFixture {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("fixture listen: %v", err)
	}
	f := &protocolFixture{
		t:                 t,
		l:                 l,
		addr:              fmt.Sprintf("nats://127.0.0.1:%d", l.Addr().(*net.TCPAddr).Port),
		subs:              make(map[string]int64),
		heartbeatInterval: heartbeatInterval,
		pullCh:            make(chan *fixturePull, 16),
		done:              make(chan struct{}),
	}
	f.wg.Add(1)
	go f.serve()
	return f
}

func (f *protocolFixture) close() {
	close(f.done)
	f.l.Close()
	f.wg.Wait()
}

func (f *protocolFixture) serve() {
	defer f.wg.Done()
	conn, err := f.l.Accept()
	if err != nil {
		select {
		case <-f.done:
			return
		default:
			f.t.Errorf("fixture accept: %v", err)
			return
		}
	}
	f.connMu.Lock()
	f.conn = conn
	f.connMu.Unlock()
	defer conn.Close()

	// Handshake: send INFO.
	info := `INFO {"server_id":"fixture","host":"127.0.0.1","port":4222,"auth_required":false,"tls_required":false,"max_payload":1048576,"proto":1,"jetstream":true}` + "\r\n"
	if _, err := conn.Write([]byte(info)); err != nil {
		f.t.Errorf("fixture write INFO: %v", err)
		return
	}

	reader := bufio.NewReader(conn)
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			return
		}
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		parts := strings.Fields(line)
		switch strings.ToUpper(parts[0]) {
		case "CONNECT":
			// discard
		case "PING":
			if _, err := conn.Write([]byte("PONG\r\n")); err != nil {
				return
			}
		case "PONG":
			// discard
		case "SUB":
			if len(parts) < 3 {
				continue
			}
			// SUB <subject> [queue] <sid>
			subject := parts[1]
			sid := parseInt64(parts[len(parts)-1])
			f.mu.Lock()
			f.subs[subject] = sid
			f.mu.Unlock()
		case "PUB":
			f.handlePub(conn, parts, reader)
		}
	}
}

func (f *protocolFixture) handlePub(conn net.Conn, parts []string, reader *bufio.Reader) {
	args := parts[1:]
	var subject, reply string
	var size int
	switch len(args) {
	case 2:
		subject = args[0]
		size = int(parseInt64(args[1]))
	case 3:
		subject = args[0]
		reply = args[1]
		size = int(parseInt64(args[2]))
	default:
		f.t.Errorf("fixture bad PUB line: %q", strings.Join(parts, " "))
		return
	}
	if size < 0 {
		f.t.Errorf("fixture bad PUB size: %d", size)
		return
	}
	payload := make([]byte, size)
	if _, err := readFull(reader, payload); err != nil {
		return
	}
	// Consume trailing \r\n.
	reader.ReadString('\n')

	switch {
	case strings.Contains(subject, "STREAM.CREATE"):
		f.respond(conn, reply, f.streamInfoJSON("test"))
	case strings.Contains(subject, "CONSUMER.CREATE"):
		f.respond(conn, reply, f.consumerInfoJSON("test", "cons"))
	case strings.Contains(subject, "CONSUMER.MSG.NEXT"):
		pull := &fixturePull{subject: subject, reply: reply}
		f.mu.Lock()
		f.latestReply = reply
		f.mu.Unlock()
		select {
		case f.pullCh <- pull:
		default:
		}
		go f.emitHeartbeats(conn, reply)
	default:
		f.respond(conn, reply, []byte(`{"type":"io.nats.jetstream.api.v1.error_response","error":{"code":503,"description":"no responders"}}`))
	}
}

// emitHeartbeats sends idle heartbeat frames (Status: 100, no payload) on
// the given reply subject until the fixture is closed.
func (f *protocolFixture) emitHeartbeats(conn net.Conn, reply string) {
	ticker := time.NewTicker(f.heartbeatInterval)
	defer ticker.Stop()
	hdr := "NATS/1.0\r\nStatus: 100\r\n\r\n"
	for {
		select {
		case <-ticker.C:
			sid := f.findSID(reply)
			f.writeHMSG(conn, reply, sid, hdr, nil)
		case <-f.done:
			return
		}
	}
}

// sendRealMessage injects a real (user) message on the latest pull request.
func (f *protocolFixture) sendRealMessage(payload []byte) {
	f.mu.Lock()
	reply := f.latestReply
	f.mu.Unlock()
	conn := f.getConn()
	if conn == nil || reply == "" {
		f.t.Error("fixture sendRealMessage: no active pull request")
		return
	}
	sid := f.findSID(reply)
	hdr := "NATS/1.0\r\n\r\n"
	f.writeHMSG(conn, reply, sid, hdr, payload)
}

func (f *protocolFixture) getConn() net.Conn {
	f.connMu.Lock()
	defer f.connMu.Unlock()
	return f.conn
}

func (f *protocolFixture) writeHMSG(conn net.Conn, reply string, sid int64, hdr string, payload []byte) {
	total := len(hdr) + len(payload)
	var b strings.Builder
	fmt.Fprintf(&b, "HMSG %s %d %d %d\r\n", reply, sid, len(hdr), total)
	b.WriteString(hdr)
	if len(payload) > 0 {
		b.Write(payload)
	}
	b.WriteString("\r\n")
	if _, err := conn.Write([]byte(b.String())); err != nil {
		f.t.Errorf("fixture write HMSG: %v", err)
	}
}

func (f *protocolFixture) respond(conn net.Conn, reply string, payload []byte) {
	sid := f.findSID(reply)
	f.writeHMSG(conn, reply, sid, "NATS/1.0\r\n\r\n", payload)
}

// findSID returns the client subscription id that matches the given subject,
// supporting NATS wildcards (* and >). Falls back to exact match.
func (f *protocolFixture) findSID(subject string) int64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	if sid, ok := f.subs[subject]; ok {
		return sid
	}
	for subj, sid := range f.subs {
		if subjectMatches(subj, subject) {
			return sid
		}
	}
	return 0
}

// subjectMatches reports whether subject matches the NATS wildcard pattern.
func subjectMatches(pattern, subject string) bool {
	pTokens := strings.Split(pattern, ".")
	sTokens := strings.Split(subject, ".")
	for i, p := range pTokens {
		if p == ">" {
			return true
		}
		if i >= len(sTokens) {
			return false
		}
		if p != "*" && p != sTokens[i] {
			return false
		}
	}
	return len(pTokens) == len(sTokens)
}

func (f *protocolFixture) streamInfoJSON(name string) []byte {
	info := map[string]any{
		"type":    "io.nats.jetstream.api.v1.stream_create_response",
		"config":  map[string]any{"name": name, "subjects": []string{name + ".>"}},
		"created": time.Now().UTC().Format(time.RFC3339),
		"state":   map[string]any{"messages": 0, "bytes": 0, "first_seq": 0, "last_seq": 0, "consumer_count": 0},
	}
	b, _ := json.Marshal(info)
	return b
}

func (f *protocolFixture) consumerInfoJSON(stream, name string) []byte {
	info := map[string]any{
		"type":        "io.nats.jetstream.api.v1.consumer_create_response",
		"stream_name": stream,
		"name":        name,
		"config":      map[string]any{"durable_name": name, "name": name},
		"created":     time.Now().UTC().Format(time.RFC3339),
		"delivered":   map[string]any{"consumer_seq": 0, "stream_seq": 0},
		"ack_floor":   map[string]any{"consumer_seq": 0, "stream_seq": 0},
	}
	b, _ := json.Marshal(info)
	return b
}

func readFull(r *bufio.Reader, buf []byte) (int, error) {
	var total int
	for total < len(buf) {
		n, err := r.Read(buf[total:])
		total += n
		if err != nil {
			return total, err
		}
	}
	return total, nil
}

func parseInt64(s string) int64 {
	var n int64
	for _, c := range s {
		if c < '0' || c > '9' {
			return -1
		}
		n = n*10 + int64(c-'0')
	}
	return n
}

// newFixtureConsumer creates a pull consumer backed by the protocol fixture.
func newFixtureConsumer(t *testing.T, f *protocolFixture) (Consumer, *nats.Conn) {
	t.Helper()
	nc, err := nats.Connect(f.addr, nats.NoReconnect(), nats.Timeout(5*time.Second))
	if err != nil {
		t.Fatalf("nats connect: %v", err)
	}
	js, err := New(nc)
	if err != nil {
		nc.Close()
		t.Fatalf("jetstream new: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := js.CreateStream(ctx, StreamConfig{Name: "test", Subjects: []string{"test.>"}}); err != nil {
		nc.Close()
		t.Fatalf("create stream: %v", err)
	}
	cons, err := js.CreateConsumer(ctx, "test", ConsumerConfig{Name: "cons", Durable: "cons"})
	if err != nil {
		nc.Close()
		t.Fatalf("create consumer: %v", err)
	}
	return cons, nc
}

// TestPullIdleDeadlineTimeout verifies that the idle deadline fires when no
// real message is received, even though heartbeats continue to arrive.
func TestPullIdleDeadlineTimeout(t *testing.T) {
	const idleTimeout = 300 * time.Millisecond
	f := newProtocolFixture(t, 100*time.Millisecond)
	defer f.close()

	cons, nc := newFixtureConsumer(t, f)
	defer nc.Close()

	iter, err := cons.Messages(PullIdleTimeout(idleTimeout), PullHeartbeat(500*time.Millisecond))
	if err != nil {
		t.Fatalf("Messages: %v", err)
	}
	defer iter.Stop()

	// Wait for the idle deadline to fire. Heartbeats are emitted automatically
	// by the fixture, so the hbMonitor stays happy while the idle deadline
	// counts down.
	start := time.Now()
	_, err = iter.Next()
	elapsed := time.Since(start)

	if !errors.Is(err, ErrIdleDeadlineExceeded) {
		t.Fatalf("expected ErrIdleDeadlineExceeded, got: %v (after %v)", err, elapsed)
	}
	// The deadline should fire at ~idleTimeout, not at the heartbeat interval.
	if elapsed < idleTimeout-50*time.Millisecond || elapsed > idleTimeout+200*time.Millisecond {
		t.Fatalf("idle deadline fired at %v, expected ~%v", elapsed, idleTimeout)
	}
	t.Logf("idle deadline fired after %v (expected ~%v)", elapsed, idleTimeout)
}

// TestPullIdleDeadlineRenewal verifies that a real message refreshes the idle
// deadline, so the consumer survives past the original deadline and only times
// out after the idle timeout elapses from the last real message.
func TestPullIdleDeadlineRenewal(t *testing.T) {
	const idleTimeout = 300 * time.Millisecond
	f := newProtocolFixture(t, 100*time.Millisecond)
	defer f.close()

	cons, nc := newFixtureConsumer(t, f)
	defer nc.Close()

	iter, err := cons.Messages(PullIdleTimeout(idleTimeout), PullHeartbeat(500*time.Millisecond))
	if err != nil {
		t.Fatalf("Messages: %v", err)
	}
	defer iter.Stop()

	// Start Next in a goroutine to trigger the pull request (the initial
	// pending count is zero, so the pull is only sent when Next is driven).
	type nextResult struct {
		msg Msg
		err error
	}
	nextCh := make(chan nextResult, 1)
	go func() {
		msg, err := iter.Next()
		nextCh <- nextResult{msg, err}
	}()

	// Wait for the pull request to be registered.
	select {
	case <-f.pullCh:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for pull request")
	}

	// Send a real message at ~150ms, which refreshes the deadline.
	time.Sleep(150 * time.Millisecond)
	f.sendRealMessage([]byte("hello"))

	// Consume the real message.
	res := <-nextCh
	if res.err != nil {
		t.Fatalf("Next after real message: %v", res.err)
	}
	if string(res.msg.Data()) != "hello" {
		t.Fatalf("expected 'hello', got %q", res.msg.Data())
	}

	// The original deadline (300ms from start) has now passed, but the real
	// message refreshed it. The consumer should time out ~300ms after the
	// real message (at ~450ms from start).
	start := time.Now()
	_, err = iter.Next()
	elapsed := time.Since(start)
	if !errors.Is(err, ErrIdleDeadlineExceeded) {
		t.Fatalf("expected ErrIdleDeadlineExceeded after renewal, got: %v (after %v)", err, elapsed)
	}
	// The timeout should be ~idleTimeout from the last real message.
	if elapsed < idleTimeout-50*time.Millisecond || elapsed > idleTimeout+200*time.Millisecond {
		t.Fatalf("idle deadline after renewal fired at %v, expected ~%v", elapsed, idleTimeout)
	}
	t.Logf("idle deadline after renewal fired after %v (expected ~%v)", elapsed, idleTimeout)
}

// TestPullIdleDeadlineCancellation verifies that after the idle deadline
// fires, no further messages are dispatched to the caller.
func TestPullIdleDeadlineCancellation(t *testing.T) {
	const idleTimeout = 200 * time.Millisecond
	f := newProtocolFixture(t, 50*time.Millisecond)
	defer f.close()

	cons, nc := newFixtureConsumer(t, f)
	defer nc.Close()

	iter, err := cons.Messages(PullIdleTimeout(idleTimeout), PullHeartbeat(500*time.Millisecond))
	if err != nil {
		t.Fatalf("Messages: %v", err)
	}
	defer iter.Stop()

	// Wait for the idle deadline to fire.
	_, err = iter.Next()
	if !errors.Is(err, ErrIdleDeadlineExceeded) {
		t.Fatalf("expected ErrIdleDeadlineExceeded, got: %v", err)
	}

	// Inject a real message after cancellation. It must not be dispatched.
	f.sendRealMessage([]byte("after-cancel"))

	// Next should report the iterator is closed, not deliver the message.
	_, err = iter.Next()
	if !errors.Is(err, ErrMsgIteratorClosed) {
		t.Fatalf("expected ErrMsgIteratorClosed after cancellation, got: %v", err)
	}
	t.Log("no messages dispatched after cancellation")
}

// TestPullIdleDeadlineDisabled verifies that without the idle deadline option,
// the consumer is not cancelled by idle time.
func TestPullIdleDeadlineDisabled(t *testing.T) {
	f := newProtocolFixture(t, 50*time.Millisecond)
	defer f.close()

	cons, nc := newFixtureConsumer(t, f)
	defer nc.Close()

	// No PullIdleTimeout option; consumer should not time out on idle.
	iter, err := cons.Messages(PullHeartbeat(500*time.Millisecond))
	if err != nil {
		t.Fatalf("Messages: %v", err)
	}
	defer iter.Stop()

	// Start Next in a goroutine to trigger the pull request (the initial
	// pending count is zero, so the pull is only sent when Next is driven).
	type nextResult struct {
		msg Msg
		err error
	}
	nextCh := make(chan nextResult, 1)
	go func() {
		msg, err := iter.Next()
		nextCh <- nextResult{msg, err}
	}()

	// Wait for the pull request to be registered.
	select {
	case <-f.pullCh:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for pull request")
	}

	// Send a real message after a short wait.
	time.Sleep(150 * time.Millisecond)
	f.sendRealMessage([]byte("still-alive"))

	res := <-nextCh
	if res.err != nil {
		t.Fatalf("Next: %v", res.err)
	}
	if string(res.msg.Data()) != "still-alive" {
		t.Fatalf("expected 'still-alive', got %q", res.msg.Data())
	}
	t.Log("consumer alive without idle deadline")
}
