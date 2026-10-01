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
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	natstest "github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats.go"
)

// startEmbeddedJS starts a real JetStream server in-process (no external
// binaries, sockets or containers) and returns its client URL. The server
// is shut down on test cleanup.
func startEmbeddedJS(t *testing.T) string {
	t.Helper()
	opts := &natsserver.Options{
		Host:      "127.0.0.1",
		Port:      -1,
		JetStream: true,
		StoreDir:  t.TempDir(),
		NoLog:     true,
	}
	s := natstest.RunServer(opts)
	t.Cleanup(func() {
		s.Shutdown()
		s.WaitForShutdown()
	})
	if !s.ReadyForConnections(5 * time.Second) {
		t.Fatalf("embedded nats-server did not become ready")
	}
	return s.ClientURL()
}

func newTestJS(t *testing.T, url string) (*nats.Conn, JetStream) {
	t.Helper()
	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatalf("nats.Connect: %v", err)
	}
	t.Cleanup(nc.Close)
	js, err := New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	return nc, js
}

// TestObjectResultStreamValidationAcceptance is the repeatable acceptance
// test for "object stream validation failure closure". It covers:
//
//   - complete object: exact byte count, nil error, subscription released;
//   - corrupted chunk: delivered bytes are readable, the terminal
//     ErrDigestMismatch latches and is reproduced by every later Read and
//     by Error(), and the subscription is released;
//   - premature stream cut (connection dropped mid-read): Read does not
//     hang or fake a clean EOF, ErrObjectIncomplete latches and reproduces,
//     subscription resources are released;
//   - explicit Close while reading: later Reads keep failing and the
//     subscription is released.
func TestObjectResultStreamValidationAcceptance(t *testing.T) {
	url := startEmbeddedJS(t)

	t.Run("complete chunks", func(t *testing.T) {
		_, js := newTestJS(t, url)
		ctx := context.Background()
		obs, err := js.CreateObjectStore(ctx, ObjectStoreConfig{Bucket: "COMPLETE"})
		if err != nil {
			t.Fatalf("CreateObjectStore: %v", err)
		}

		// 3 chunks of 64B.
		payload := bytes.Repeat([]byte{'a'}, 3*64)
		_, err = obs.Put(ctx, ObjectMeta{
			Name: "obj",
			Opts: &ObjectMetaOptions{ChunkSize: 64},
		}, bytes.NewReader(payload))
		if err != nil {
			t.Fatalf("Put: %v", err)
		}

		res, err := obs.Get(ctx, "obj")
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		or := res.(*objResult)

		got, err := io.ReadAll(res)
		if err != nil {
			t.Fatalf("ReadAll: %v", err)
		}
		if len(got) != len(payload) {
			t.Fatalf("byte count: got %d, want %d", len(got), len(payload))
		}
		if !bytes.Equal(got, payload) {
			t.Fatalf("payload mismatch")
		}
		if err := res.Error(); err != nil {
			t.Fatalf("Error after complete read: %v", err)
		}

		// Repeated Reads after a clean stream keep returning io.EOF.
		if _, err := res.Read(make([]byte, 16)); !errors.Is(err, io.EOF) {
			t.Fatalf("Read after EOF: got %v, want io.EOF", err)
		}

		// Terminal teardown released the subscription.
		checkSubReleased(t, or)
		if !or.finished {
			t.Fatalf("result not marked finished after full read")
		}
		if err := res.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
	})

	t.Run("corrupted chunk latches digest error and releases", func(t *testing.T) {
		nc, js := newTestJS(t, url)
		ctx := context.Background()
		obs, err := js.CreateObjectStore(ctx, ObjectStoreConfig{Bucket: "CORRUPT"})
		if err != nil {
			t.Fatalf("CreateObjectStore: %v", err)
		}

		payload := []byte("abc")
		info, err := obs.PutString(ctx, "obj", string(payload))
		if err != nil {
			t.Fatalf("PutString: %v", err)
		}

		// Append an extra chunk on the object's chunk subject after Put():
		// the metadata still describes 3 bytes so the digest must mismatch.
		subj := fmt.Sprintf("$O.CORRUPT.C.%s", info.NUID)
		if _, err := js.Publish(ctx, subj, []byte("123")); err != nil {
			t.Fatalf("Publish corrupt chunk: %v", err)
		}
		if !nc.IsConnected() {
			t.Fatalf("setup connection dropped")
		}

		res, err := obs.Get(ctx, "obj")
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		or := res.(*objResult)

		got, err := io.ReadAll(res)
		// All chunks that arrived remain readable ("abc" + the injected
		// "123"), but the stream check fails at the end and must not claim
		// success despite the meta describing 3 bytes.
		if !errors.Is(err, ErrDigestMismatch) {
			t.Fatalf("ReadAll error: got %v, want ErrDigestMismatch", err)
		}
		if string(got) != "abc123" {
			t.Fatalf("delivered bytes: got %q, want %q", string(got), "abc123")
		}
		if n := len(got); n != 6 {
			t.Fatalf("byte count: got %d, want 6", n)
		}

		// The terminal error must reproduce on later Reads and via Error().
		if _, err := res.Read(make([]byte, 8)); !errors.Is(err, ErrDigestMismatch) {
			t.Fatalf("second Read: got %v, want ErrDigestMismatch", err)
		}
		if _, err := res.Read(make([]byte, 8)); !errors.Is(err, ErrDigestMismatch) {
			t.Fatalf("third Read: got %v, want ErrDigestMismatch", err)
		}
		if !errors.Is(res.Error(), ErrDigestMismatch) {
			t.Fatalf("Error(): got %v, want ErrDigestMismatch", res.Error())
		}
		info2, infoErr := res.Info()
		if info2 == nil || !errors.Is(infoErr, ErrDigestMismatch) {
			t.Fatalf("Info(): info=%v err=%v, want ErrDigestMismatch", info2, infoErr)
		}

		// GetBytes-style callers must not get a silently truncated object.
		if _, err := obs.GetBytes(ctx, "obj"); !errors.Is(err, ErrDigestMismatch) {
			t.Fatalf("GetBytes on corrupt object: got %v, want ErrDigestMismatch", err)
		}

		// Subscription resources released by the terminal callback.
		checkSubReleased(t, or)
		if err := res.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
	})

	t.Run("premature stream cut surfaces terminal error and releases", func(t *testing.T) {
		// Dedicated connection for the object reader so we can sever it
		// mid-stream without affecting the server or the bucket.
		nc2, err := nats.Connect(url)
		if err != nil {
			t.Fatalf("nats.Connect: %v", err)
		}
		js2, err := New(nc2)
		if err != nil {
			t.Fatalf("jetstream.New: %v", err)
		}
		ctx := context.Background()
		obs, err := js2.CreateObjectStore(ctx, ObjectStoreConfig{Bucket: "CUTOFF"})
		if err != nil {
			t.Fatalf("CreateObjectStore: %v", err)
		}

		// The object must outgrow the subscription's client-side prefetch
		// buffer (DefaultSubPendingBytesLimit = 64MB) so a blocked Read is
		// genuinely waiting on the transport when the stream is cut.
		const chunkSize = 1024
		payload := bytes.Repeat([]byte{'b'}, 70*1024*1024)
		_, err = obs.Put(ctx, ObjectMeta{
			Name: "obj",
			Opts: &ObjectMetaOptions{ChunkSize: chunkSize},
		}, bytes.NewReader(payload))
		if err != nil {
			t.Fatalf("Put: %v", err)
		}

		res, err := obs.Get(ctx, "obj")
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		or := res.(*objResult)

		// Drain in a goroutine through a counting writer. Once it has made
		// progress, block the writer so the next Read parks waiting on the
		// transport; then sever the connection.
		cw := &countingWriter{}
		copyErr := make(chan error, 1)
		proceed := make(chan struct{})
		release := make(chan struct{})
		go func() {
			_, rerr := io.Copy(cw.writer(proceed, release), res)
			copyErr <- rerr
		}()
		<-proceed // one chunk delivered, writer now blocked
		// Sever the transport, then release the writer so io.Copy issues its
		// next Read, which must surface the terminal error instead of EOF.
		nc2.Close()
		time.Sleep(200 * time.Millisecond)
		close(release)

		select {
		case err := <-copyErr:
			if err == nil {
				t.Fatalf("stream copy returned nil error after stream cut")
			}
			if errors.Is(err, io.EOF) {
				t.Fatalf("stream copy faked a clean io.EOF after stream cut")
			}
			if !errors.Is(err, ErrObjectIncomplete) {
				t.Fatalf("stream copy: got %v, want ErrObjectIncomplete", err)
			}
		case <-time.After(15 * time.Second):
			t.Fatalf("stream copy did not return after stream was cut")
		}
		if cw.n() == 0 {
			t.Fatalf("no bytes delivered before stream cut")
		}
		t.Logf("received %d of %d bytes before stream cut", cw.n(), len(payload))

		// Later Reads reproduce the same terminal error...
		if _, err := res.Read(make([]byte, chunkSize)); !errors.Is(err, ErrObjectIncomplete) {
			t.Fatalf("later Read: got %v, want ErrObjectIncomplete", err)
		}
		if !errors.Is(res.Error(), ErrObjectIncomplete) {
			t.Fatalf("Error(): got %v, want ErrObjectIncomplete", res.Error())
		}
		// ...and the subscription is no longer active.
		if or.sub != nil && or.sub.IsValid() {
			t.Fatalf("subscription still valid after stream was cut")
		}
		if !or.finished {
			t.Fatalf("result not marked finished after stream cut")
		}
		if err := res.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
	})

	t.Run("context cancel mid-read surfaces terminal error and releases", func(t *testing.T) {
		_, js := newTestJS(t, url)
		ctx, cancel := context.WithCancel(context.Background())
		obs, err := js.CreateObjectStore(ctx, ObjectStoreConfig{Bucket: "CANCEL"})
		if err != nil {
			t.Fatalf("CreateObjectStore: %v", err)
		}

		const chunkSize = 1024
		payload := bytes.Repeat([]byte{'d'}, 70*1024*1024)
		_, err = obs.Put(context.Background(), ObjectMeta{
			Name: "obj",
			Opts: &ObjectMetaOptions{ChunkSize: chunkSize},
		}, bytes.NewReader(payload))
		if err != nil {
			t.Fatalf("Put: %v", err)
		}

		res, err := obs.Get(ctx, "obj")
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		or := res.(*objResult)

		cw := &countingWriter{}
		copyErr := make(chan error, 1)
		proceed := make(chan struct{})
		release := make(chan struct{})
		go func() {
			_, rerr := io.Copy(cw.writer(proceed, release), res)
			copyErr <- rerr
		}()
		<-proceed
		cancel()
		time.Sleep(200 * time.Millisecond)
		close(release)

		select {
		case err := <-copyErr:
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("stream copy: got %v, want context.Canceled", err)
			}
		case <-time.After(10 * time.Second):
			t.Fatalf("stream copy did not return after cancel")
		}
		t.Logf("received %d of %d bytes before cancel", cw.n(), len(payload))

		// The cancellation error latches and reproduces.
		if _, err := res.Read(make([]byte, chunkSize)); !errors.Is(err, context.Canceled) {
			t.Fatalf("later Read: got %v, want context.Canceled", err)
		}
		checkSubReleased(t, or)
		if !or.finished {
			t.Fatalf("result not marked finished after cancel")
		}
		if err := res.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
	})

	t.Run("close mid-read releases subscription and keeps failing", func(t *testing.T) {
		_, js := newTestJS(t, url)
		ctx := context.Background()
		obs, err := js.CreateObjectStore(ctx, ObjectStoreConfig{Bucket: "CLOSE"})
		if err != nil {
			t.Fatalf("CreateObjectStore: %v", err)
		}

		// 128 bytes in a single chunk: serve one partial Read while data
		// remains, then Close out the result.
		payload := bytes.Repeat([]byte{'c'}, 128)
		_, err = obs.Put(ctx, ObjectMeta{Name: "obj"}, bytes.NewReader(payload))
		if err != nil {
			t.Fatalf("Put: %v", err)
		}

		res, err := obs.Get(ctx, "obj")
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		or := res.(*objResult)

		p := make([]byte, 32)
		n, rerr := res.Read(p)
		if rerr != nil {
			t.Fatalf("first Read: %v", rerr)
		}
		if n != 32 {
			t.Fatalf("first read byte count: got %d, want 32", n)
		}

		if err := res.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}

		// Every later Read must report the closed terminal state rather than
		// more data or a clean EOF.
		for i := 0; i < 2; i++ {
			if _, err := res.Read(make([]byte, 32)); !errors.Is(err, io.ErrClosedPipe) {
				t.Fatalf("Read after Close #%d: got %v, want io.ErrClosedPipe", i, err)
			}
		}
		if or.sub != nil && or.sub.IsValid() {
			t.Fatalf("subscription still valid after Close")
		}
		if !or.finished {
			t.Fatalf("result not marked finished after Close")
		}
		// Close is idempotent.
		if err := res.Close(); err != nil {
			t.Fatalf("second Close: %v", err)
		}
	})
}

// countingWriter gates delivery: the first Write signals proceed and stalls
// on release. After release is closed, further Writes pass straight through,
// letting io.Copy reach its next (blocked) Read at the chosen moment.
type countingWriter struct {
	total int64
}

func (c *countingWriter) writer(proceed chan<- struct{}, release <-chan struct{}) io.Writer {
	signaled := false
	return writerFunc(func(p []byte) (int, error) {
		c.total += int64(len(p))
		if !signaled {
			signaled = true
			proceed <- struct{}{}
			<-release
		}
		return len(p), nil
	})
}

func (c *countingWriter) n() int64 { return c.total }

type writerFunc func(p []byte) (int, error)

func (f writerFunc) Write(p []byte) (int, error) { return f(p) }

// checkSubReleased waits until the result's chunk subscription is gone.
func checkSubReleased(t *testing.T, or *objResult) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		or.Lock()
		sub := or.sub
		or.Unlock()
		if sub == nil || !sub.IsValid() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("subscription still valid after terminal state")
}

// TestObjectResultZeroSize verifies the zero-size fast path (no pipe, no
// subscription) still behaves like an empty reader.
func TestObjectResultZeroSize(t *testing.T) {
	_, js := newTestJS(t, startEmbeddedJS(t))
	ctx := context.Background()
	obs, err := js.CreateObjectStore(ctx, ObjectStoreConfig{Bucket: "ZERO"})
	if err != nil {
		t.Fatalf("CreateObjectStore: %v", err)
	}
	if _, err := obs.PutString(ctx, "empty", ""); err != nil {
		t.Fatalf("PutString: %v", err)
	}
	res, err := obs.Get(ctx, "empty")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if _, err := io.ReadAll(res); err != nil {
		t.Fatalf("ReadAll empty object: %v", err)
	}
	if err := res.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}
