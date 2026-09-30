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
	"bytes"
	"io"
	"net"
	"sync"
	"testing"
	"time"
)

type fakeConn struct {
	mu       sync.Mutex
	closed   bool
	readBuf  *bytes.Buffer
	writeBuf *bytes.Buffer
	cond     *sync.Cond
}

func newFakeConn() *fakeConn {
	fc := &fakeConn{
		readBuf:  new(bytes.Buffer),
		writeBuf: new(bytes.Buffer),
	}
	fc.cond = sync.NewCond(&fc.mu)
	info := "INFO {\"server_id\":\"fake\",\"version\":\"1.0.0\",\"go\":\"go\",\"host\":\"127.0.0.1\",\"port\":4222,\"auth_required\":false,\"ssl_required\":false,\"tls_required\":false,\"tls_verify\":false,\"max_payload\":1048576,\"proto\":1}\r\n"
	fc.readBuf.WriteString(info)
	return fc
}

func (fc *fakeConn) Read(b []byte) (int, error) {
	fc.mu.Lock()
	defer fc.mu.Unlock()
	for fc.readBuf.Len() == 0 {
		if fc.closed {
			return 0, io.EOF
		}
		fc.cond.Wait()
	}
	return fc.readBuf.Read(b)
}

func (fc *fakeConn) Write(p []byte) (int, error) {
	fc.mu.Lock()
	defer fc.mu.Unlock()
	if fc.closed {
		return 0, io.EOF
	}
	n, err := fc.writeBuf.Write(p)
	if bytes.Contains(p, []byte("PING\r\n")) {
		fc.readBuf.WriteString("PONG\r\n")
		fc.cond.Signal()
	}
	return n, err
}

func (fc *fakeConn) Close() error {
	fc.mu.Lock()
	defer fc.mu.Unlock()
	fc.closed = true
	fc.cond.Broadcast()
	return nil
}

func (fc *fakeConn) LocalAddr() net.Addr {
	return &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 4222}
}

func (fc *fakeConn) RemoteAddr() net.Addr {
	return &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 4222}
}

func (fc *fakeConn) SetDeadline(t time.Time) error      { return nil }
func (fc *fakeConn) SetReadDeadline(t time.Time) error  { return nil }
func (fc *fakeConn) SetWriteDeadline(t time.Time) error { return nil }

type fakeDialer struct {
	mu       sync.Mutex
	conns    []*fakeConn
	failNext bool
}

func (d *fakeDialer) Dial(network, address string) (net.Conn, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.failNext {
		return nil, io.EOF
	}
	fc := newFakeConn()
	d.conns = append(d.conns, fc)
	return fc, nil
}

func (d *fakeDialer) lastConn() *fakeConn {
	d.mu.Lock()
	defer d.mu.Unlock()
	if len(d.conns) == 0 {
		return nil
	}
	return d.conns[len(d.conns)-1]
}

func (d *fakeDialer) setFailNext(fail bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.failNext = fail
}

func waitForStatus(t *testing.T, nc *Conn, expected Status, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if nc.Status() == expected {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for status %s, got %s", expected, nc.Status())
}

func waitForReconnectRecovery(t *testing.T, nc *Conn, expectedRecovered, expectedPending int, timeout time.Duration) *ReconnectRecoveryList {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var list *ReconnectRecoveryList
	for time.Now().Before(deadline) {
		list = nc.ReconnectRecoveryList()
		if list != nil && len(list.Recovered()) == expectedRecovered && len(list.Pending()) == expectedPending {
			return list
		}
		time.Sleep(5 * time.Millisecond)
	}
	if list == nil {
		t.Fatalf("timed out waiting for recovery list, got nil")
	}
	t.Fatalf("timed out waiting for recovery list: expected %d recovered, %d pending; got %d recovered, %d pending",
		expectedRecovered, expectedPending, len(list.Recovered()), len(list.Pending()))
	return nil
}

func TestReconnectRecoveryListNormal(t *testing.T) {
	dialer := &fakeDialer{}
	nc, err := Connect("nats://fake:4222",
		SetCustomDialer(dialer),
		MaxReconnects(-1),
		ReconnectWait(10*time.Millisecond),
		ReconnectJitter(0, 0),
		PingInterval(200*time.Millisecond),
		SkipHostLookup(),
	)
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer nc.Close()

	subs := make([]*Subscription, 5)
	for i := 0; i < 5; i++ {
		sub, err := nc.Subscribe("test.subject", func(msg *Msg) {})
		if err != nil {
			t.Fatalf("Failed to subscribe: %v", err)
		}
		subs[i] = sub
	}

	if list := nc.ReconnectRecoveryList(); list != nil {
		t.Fatalf("Expected nil recovery list before reconnect, got %+v", list)
	}

	dialer.lastConn().Close()
	list := waitForReconnectRecovery(t, nc, 5, 0, 2*time.Second)

	recovered := list.Recovered()
	if len(recovered) != 5 {
		t.Fatalf("Expected 5 recovered, got %d", len(recovered))
	}
	for i := 1; i < len(recovered); i++ {
		if recovered[i-1] >= recovered[i] {
			t.Fatalf("Expected stable order, got %v", recovered)
		}
	}
	if len(list.Pending()) != 0 {
		t.Fatalf("Expected 0 pending, got %d", len(list.Pending()))
	}
	for i, sub := range subs {
		if recovered[i] != sub.sid {
			t.Fatalf("Expected SID %d at index %d, got %d", sub.sid, i, recovered[i])
		}
	}
}

func TestReconnectRecoveryListCancelDuringReconnect(t *testing.T) {
	dialer := &fakeDialer{}
	nc, err := Connect("nats://fake:4222",
		SetCustomDialer(dialer),
		MaxReconnects(-1),
		ReconnectWait(10*time.Millisecond),
		ReconnectJitter(0, 0),
		PingInterval(200*time.Millisecond),
		SkipHostLookup(),
	)
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer nc.Close()

	subs := make([]*Subscription, 5)
	for i := 0; i < 5; i++ {
		sub, err := nc.Subscribe("test.subject", func(msg *Msg) {})
		if err != nil {
			t.Fatalf("Failed to subscribe: %v", err)
		}
		subs[i] = sub
	}

	dialer.lastConn().Close()
	waitForStatus(t, nc, RECONNECTING, time.Second)

	if err := subs[1].Unsubscribe(); err != nil {
		t.Fatalf("Failed to unsubscribe: %v", err)
	}
	if err := subs[3].Unsubscribe(); err != nil {
		t.Fatalf("Failed to unsubscribe: %v", err)
	}

	list := waitForReconnectRecovery(t, nc, 3, 0, 2*time.Second)

	recovered := list.Recovered()
	canceledSIDs := map[int64]bool{subs[1].sid: true, subs[3].sid: true}
	for _, sid := range recovered {
		if canceledSIDs[sid] {
			t.Fatalf("Canceled subscription SID %d should not be recovered", sid)
		}
	}

	expectedSIDs := []int64{subs[0].sid, subs[2].sid, subs[4].sid}
	if len(recovered) != len(expectedSIDs) {
		t.Fatalf("Expected %d recovered, got %d", len(expectedSIDs), len(recovered))
	}
	for i, sid := range recovered {
		if sid != expectedSIDs[i] {
			t.Fatalf("Expected SID %d at index %d, got %d", expectedSIDs[i], i, sid)
		}
	}
}

func TestReconnectRecoveryListFailure(t *testing.T) {
	dialer := &fakeDialer{}
	nc, err := Connect("nats://fake:4222",
		SetCustomDialer(dialer),
		MaxReconnects(3),
		ReconnectWait(10*time.Millisecond),
		ReconnectJitter(0, 0),
		PingInterval(200*time.Millisecond),
		SkipHostLookup(),
	)
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer nc.Close()

	for i := 0; i < 3; i++ {
		if _, err := nc.Subscribe("test.subject", func(msg *Msg) {}); err != nil {
			t.Fatalf("Failed to subscribe: %v", err)
		}
	}

	dialer.setFailNext(true)
	dialer.lastConn().Close()
	waitForStatus(t, nc, CLOSED, 2*time.Second)

	list := nc.ReconnectRecoveryList()
	if list == nil {
		t.Fatalf("Expected recovery list, got nil")
	}
	if len(list.Pending()) != 3 {
		t.Fatalf("Expected 3 pending, got %d", len(list.Pending()))
	}
	if len(list.Recovered()) != 0 {
		t.Fatalf("Expected 0 recovered, got %d", len(list.Recovered()))
	}

	pending := list.Pending()
	for i := 1; i < len(pending); i++ {
		if pending[i-1] >= pending[i] {
			t.Fatalf("Expected stable order, got %v", pending)
		}
	}
}

func TestReconnectRecoveryListNoCredentials(t *testing.T) {
	dialer := &fakeDialer{}
	nc, err := Connect("nats://fake:4222",
		SetCustomDialer(dialer),
		MaxReconnects(-1),
		ReconnectWait(10*time.Millisecond),
		ReconnectJitter(0, 0),
		PingInterval(200*time.Millisecond),
		SkipHostLookup(),
	)
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer nc.Close()

	if _, err := nc.Subscribe("secret.password", func(msg *Msg) {}); err != nil {
		t.Fatalf("Failed to subscribe: %v", err)
	}
	if _, err := nc.QueueSubscribe("secret.queue", "sensitive_group", func(msg *Msg) {}); err != nil {
		t.Fatalf("Failed to subscribe: %v", err)
	}

	dialer.lastConn().Close()
	list := waitForReconnectRecovery(t, nc, 2, 0, 2*time.Second)

	recovered := list.Recovered()
	if len(recovered) != 2 {
		t.Fatalf("Expected 2 recovered, got %d", len(recovered))
	}
	for _, sid := range recovered {
		if sid <= 0 {
			t.Fatalf("Expected positive SID, got %d", sid)
		}
	}
}
