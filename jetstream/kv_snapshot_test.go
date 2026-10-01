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
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
)

// runInProcessNATSServer starts a native in-process nats-server with
// JetStream enabled (no Docker/external services).
func runInProcessNATSServer(t *testing.T) *natsserver.Server {
	t.Helper()
	opts := &natsserver.Options{
		Host:      "127.0.0.1",
		Port:      -1,
		JetStream: true,
		StoreDir:  t.TempDir(),
	}
	srv, err := natsserver.NewServer(opts)
	if err != nil {
		t.Fatalf("nats-server: %v", err)
	}
	go srv.Start()
	if !srv.ReadyForConnections(10 * time.Second) {
		t.Fatalf("nats-server did not become ready")
	}
	t.Cleanup(srv.Shutdown)
	return srv
}

func jsForTest(t *testing.T) JetStream {
	t.Helper()
	srv := runInProcessNATSServer(t)
	nc, err := nats.Connect(srv.ClientURL())
	if err != nil {
		t.Fatalf("nats.Connect: %v", err)
	}
	t.Cleanup(nc.Close)
	js, err := New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	return js
}

// withPreSubscribeHook is a test-only WatchOpt that injects fn into the
// watcher startup window: it runs after the SnapshotBoundary sequence has
// been pinned and immediately before the consumer is created.
func withPreSubscribeHook(fn func()) WatchOpt {
	return watchOptFn(func(opts *watchOpts) error {
		opts.preSubscribeHook = fn
		return nil
	})
}

// drained holds the entries delivered before (snapshot) and after (live) the
// nil end-of-snapshot marker.
type drained struct {
	snapshot []KeyValueEntry // entries before the marker
	live     []KeyValueEntry // entries after the marker
	markerAt time.Time
}

// drainWatcher collects entries up to the nil marker, then any entries that
// arrive within liveWindow.
func drainWatcher(t *testing.T, w KeyWatcher, liveWindow time.Duration) drained {
	t.Helper()
	var d drained
	updates := w.Updates()
	for {
		select {
		case e, ok := <-updates:
			if !ok {
				t.Fatalf("updates channel closed before the snapshot marker")
			}
			if e == nil {
				d.markerAt = time.Now()
				goto live
			}
			d.snapshot = append(d.snapshot, e)
		case <-time.After(5 * time.Second):
			t.Fatalf("timed out waiting for the snapshot marker")
		}
	}
live:
	deadline := time.After(liveWindow)
	for {
		select {
		case e, ok := <-updates:
			if !ok {
				return d
			}
			if e == nil {
				t.Fatalf("received a second nil marker")
			}
			d.live = append(d.live, e)
		case <-deadline:
			return d
		}
	}
}

// TestKVSnapshotBoundaryQuiescent: a quiet bucket snapshots the latest value
// per key with revisions up to the boundary, and later updates are live.
func TestKVSnapshotBoundaryQuiescent(t *testing.T) {
	ctx := context.Background()
	js := jsForTest(t)
	kv, err := js.CreateKeyValue(ctx, KeyValueConfig{Bucket: "SNAP"})
	if err != nil {
		t.Fatalf("CreateKeyValue: %v", err)
	}

	const keys = 20
	for i := 0; i < keys; i++ {
		if _, err := kv.PutString(ctx, fmt.Sprintf("k-%02d", i), fmt.Sprintf("v0-%d", i)); err != nil {
			t.Fatalf("Put: %v", err)
		}
	}

	w, err := kv.WatchAll(ctx, SnapshotBoundary())
	if err != nil {
		t.Fatalf("WatchAll: %v", err)
	}
	defer w.Stop()

	// Keep a reader going so callback delivery never blocks; the marker
	// partitions snapshot entries from live entries.
	var (
		mu         sync.Mutex
		d          drained
		markerSeen bool
	)
	go func() {
		for e := range w.Updates() {
			mu.Lock()
			if e == nil {
				markerSeen = true
			} else if markerSeen {
				d.live = append(d.live, e)
			} else {
				d.snapshot = append(d.snapshot, e)
			}
			mu.Unlock()
		}
	}()

	checkFor2s := func(f func() bool, msg string) {
		deadline := time.Now().Add(3 * time.Second)
		for time.Now().Before(deadline) {
			mu.Lock()
			ok := f()
			mu.Unlock()
			if ok {
				return
			}
			time.Sleep(10 * time.Millisecond)
		}
		t.Fatal(msg)
	}

	// Wait for the marker with exactly the 20 snapshot entries.
	checkFor2s(func() bool { return markerSeen && len(d.snapshot) == keys },
		"marker not received with the full snapshot")
	mu.Lock()
	if len(d.live) != 0 {
		mu.Unlock()
		t.Fatalf("expected no live updates before writes, got %d", len(d.live))
	}
	lastSnapRev := d.snapshot[len(d.snapshot)-1].Revision()
	for i, e := range d.snapshot {
		if want := uint64(i + 1); e.Revision() != want {
			mu.Unlock()
			t.Fatalf("snapshot[%d] revision: got %d, want %d", i, e.Revision(), want)
		}
	}
	mu.Unlock()

	// Updates after the marker are reported strictly as live.
	for i := 0; i < 5; i++ {
		if _, err := kv.PutString(ctx, fmt.Sprintf("live-%d", i), "x"); err != nil {
			t.Fatalf("Put live: %v", err)
		}
	}
	checkFor2s(func() bool { return len(d.live) == 5 }, "5 live updates not delivered")
	mu.Lock()
	defer mu.Unlock()
	for _, e := range d.live {
		if e.Revision() <= lastSnapRev {
			t.Fatalf("live entry revision %d is not above boundary %d", e.Revision(), lastSnapRev)
		}
	}
}

// TestKVSnapshotBoundaryStartupWindow is the core acceptance test: writes are
// deterministically interleaved into the startup window (after the boundary
// sequence is pinned, before the consumer exists). Those updates must be
// delivered exactly once, as live updates, after the marker; snapshot keys
// before the marker stay at their snapshot revisions.
func TestKVSnapshotBoundaryStartupWindow(t *testing.T) {
	ctx := context.Background()
	js := jsForTest(t)
	kv, err := js.CreateKeyValue(ctx, KeyValueConfig{Bucket: "RACE"})
	if err != nil {
		t.Fatalf("CreateKeyValue: %v", err)
	}

	const snapshotKeys = 50
	for i := 0; i < snapshotKeys; i++ {
		if _, err := kv.PutString(ctx, fmt.Sprintf("snap-%02d", i), "snap"); err != nil {
			t.Fatalf("Put snap: %v", err)
		}
	}

	startWrite := make(chan struct{})
	writesDone := make(chan struct{})
	go func() {
		<-startWrite
		defer close(writesDone)
		// All writes go to fresh keys: this keeps the server-side
		// DeliverLastPerSubject replay count exact so no revision can be
		// collapsed, lost or duplicated.
		for i := 0; i < snapshotKeys; i++ {
			if _, err := kv.PutString(ctx, fmt.Sprintf("live-%02d", i), "live"); err != nil {
				t.Errorf("Put live key: %v", err)
				return
			}
		}
	}()

	w, err := kv.WatchAll(ctx, SnapshotBoundary(), withPreSubscribeHook(func() {
		close(startWrite)
	}))
	if err != nil {
		t.Fatalf("WatchAll: %v", err)
	}
	defer w.Stop()

	select {
	case <-writesDone:
	case <-time.After(5 * time.Second):
		t.Fatalf("startup-window writes did not finish")
	}

	d := drainWatcher(t, w, 2*time.Second)

	if len(d.snapshot) != snapshotKeys {
		t.Fatalf("snapshot entries: got %d, want %d (a startup-window update leaked or a snapshot was lost)",
			len(d.snapshot), snapshotKeys)
	}

	seen := make(map[uint64]bool)
	boundary := uint64(snapshotKeys)
	for i, e := range d.snapshot {
		if e.Revision() != uint64(i+1) {
			t.Fatalf("snapshot order/version: snapshot[%d] revision=%d want %d", i, e.Revision(), i+1)
		}
		if seen[e.Revision()] {
			t.Fatalf("duplicated snapshot revision %d", e.Revision())
		}
		seen[e.Revision()] = true
		if string(e.Value()) != "snap" {
			t.Fatalf("snapshot key %q revision %d has startup-window value %q (must be snapshot value)",
				e.Key(), e.Revision(), e.Value())
		}
		if e.Revision() > boundary {
			t.Fatalf("snapshot revision %d above pinned boundary %d", e.Revision(), boundary)
		}
	}

	if len(d.live) != snapshotKeys {
		t.Fatalf("live entries: got %d, want %d (a startup-window update was lost or duplicated)",
			len(d.live), snapshotKeys)
	}
	for i, e := range d.live {
		want := boundary + uint64(i) + 1
		if e.Revision() != want {
			t.Fatalf("live order/version: live[%d] revision=%d want %d", i, e.Revision(), want)
		}
		if seen[e.Revision()] {
			t.Fatalf("revision %d delivered twice (snapshot+live)", e.Revision())
		}
		seen[e.Revision()] = true
		if string(e.Value()) != "live" {
			t.Fatalf("live[%d] value=%q want %q", i, e.Value(), "live")
		}
	}

	// Every revision 1..100 was delivered exactly once, in order:
	// revisions 1..50 before the marker, 51..100 after it.
	if len(seen) != 2*snapshotKeys {
		t.Fatalf("distinct revisions delivered: got %d, want %d", len(seen), 2*snapshotKeys)
	}
}

// TestKVSnapshotBoundaryDefaultContrast deterministically interleaves writes
// into the watcher startup window: the writes are performed synchronously
// inside the pre-subscribe hook, i.e. after the boundary sequence would be
// pinned but before the consumer is created.
//
//   - The default watcher creates its consumer after the writes, so they are
//     part of the replay it considers "initial values" and are all delivered
//     BEFORE its nil marker (200 entries, marker last).
//   - The boundary watcher pinned the snapshot at revision 100 before the
//     writes, so the same 100 updates are delivered strictly AFTER its marker
//     (100 snapshot, marker, 100 live), in order, exactly once.
func TestKVSnapshotBoundaryDefaultContrast(t *testing.T) {
	ctx := context.Background()
	const n = 100

	collect := func(t *testing.T, useBoundary bool) (snapshot, live int, revs []uint64) {
		js := jsForTest(t)
		kv, err := js.CreateKeyValue(ctx, KeyValueConfig{Bucket: "CMP"})
		if err != nil {
			t.Fatalf("CreateKeyValue: %v", err)
		}
		for i := 0; i < n; i++ {
			if _, err := kv.PutString(ctx, fmt.Sprintf("snap-%03d", i), "snap"); err != nil {
				t.Fatalf("Put: %v", err)
			}
		}

		// Runs after the boundary sequence is pinned (boundary watcher) and
		// before the consumer exists for either watcher.
		hook := func() {
			for i := 0; i < n; i++ {
				if _, err := kv.PutString(ctx, fmt.Sprintf("live-%03d", i), "live"); err != nil {
					t.Errorf("Put live: %v", err)
					return
				}
			}
		}

		var w KeyWatcher
		if useBoundary {
			w, err = kv.WatchAll(ctx, SnapshotBoundary(), withPreSubscribeHook(hook))
		} else {
			w, err = kv.WatchAll(ctx, withPreSubscribeHook(hook))
		}
		if err != nil {
			t.Fatalf("WatchAll: %v", err)
		}
		defer w.Stop()

		markerSeen := false
	loop:
		for {
			select {
			case e, ok := <-w.Updates():
				if !ok {
					break loop
				}
				if e == nil {
					markerSeen = true
					continue
				}
				revs = append(revs, e.Revision())
				if markerSeen {
					live++
				} else {
					snapshot++
				}
			case <-time.After(3 * time.Second):
				break loop
			}
		}
		return snapshot, live, revs
	}

	t.Run("boundary watcher reports startup writes after the marker", func(t *testing.T) {
		snapshot, live, revs := collect(t, true)
		if snapshot != n {
			t.Fatalf("boundary watcher: snapshot entries = %d, want %d", snapshot, n)
		}
		if live != n {
			t.Fatalf("boundary watcher: live entries = %d, want %d", live, n)
		}
		if len(revs) != 2*n {
			t.Fatalf("boundary watcher: total entries = %d, want %d (a startup update was lost or duplicated)", len(revs), 2*n)
		}
		for i := 1; i < len(revs); i++ {
			if revs[i] <= revs[i-1] {
				t.Fatalf("revision order broken at %d: %d <= %d", i, revs[i], revs[i-1])
			}
		}
		t.Logf("BOUNDARY: revisions 1..%d before the marker, marker, revisions %d..%d after; %d snapshot / %d live",
			n, n+1, 2*n, snapshot, live)
	})

	t.Run("default watcher folds startup writes into initial values", func(t *testing.T) {
		snapshot, live, _ := collect(t, false)
		if snapshot != 2*n || live != 0 {
			t.Fatalf("default watcher: entries before marker = %d, after = %d; want before=%d after=0",
				snapshot, live, 2*n)
		}
		t.Logf("DEFAULT: all %d startup-window updates are reported BEFORE the marker; marker arrives only after revision %d",
			snapshot, 2*n)
	})
}

// TestKVSnapshotBoundaryUpdatesOnly: with UpdatesOnly there is no snapshot, so
func TestKVSnapshotBoundaryUpdatesOnly(t *testing.T) {
	ctx := context.Background()
	js := jsForTest(t)
	kv, err := js.CreateKeyValue(ctx, KeyValueConfig{Bucket: "UO"})
	if err != nil {
		t.Fatalf("CreateKeyValue: %v", err)
	}
	if _, err := kv.PutString(ctx, "before", "x"); err != nil {
		t.Fatalf("Put: %v", err)
	}
	w, err := kv.WatchAll(ctx, SnapshotBoundary(), UpdatesOnly())
	if err != nil {
		t.Fatalf("WatchAll: %v", err)
	}
	defer w.Stop()

	select {
	case e, ok := <-w.Updates():
		if !ok || e != nil {
			t.Fatalf("expected immediate nil marker, got %v ok=%v", e, ok)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("marker not delivered for UpdatesOnly+SnapshotBoundary")
	}

	if _, err := kv.PutString(ctx, "after", "y"); err != nil {
		t.Fatalf("Put: %v", err)
	}
	select {
	case e := <-w.Updates():
		if e == nil || e.Key() != "after" {
			t.Fatalf("expected live update for 'after', got %v", e)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("live update not delivered")
	}
}

// TestKVSnapshotBoundaryEmptyBucket: an empty bucket yields an immediate
// marker, and only subsequent writes appear afterwards.
func TestKVSnapshotBoundaryEmptyBucket(t *testing.T) {
	ctx := context.Background()
	js := jsForTest(t)
	kv, err := js.CreateKeyValue(ctx, KeyValueConfig{Bucket: "EMPTY"})
	if err != nil {
		t.Fatalf("CreateKeyValue: %v", err)
	}
	w, err := kv.WatchAll(ctx, SnapshotBoundary())
	if err != nil {
		t.Fatalf("WatchAll: %v", err)
	}
	defer w.Stop()

	select {
	case e, ok := <-w.Updates():
		if !ok || e != nil {
			t.Fatalf("expected immediate nil marker, got %v ok=%v", e, ok)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("marker not delivered for empty bucket")
	}

	if _, err := kv.PutString(ctx, "k", "v"); err != nil {
		t.Fatalf("Put: %v", err)
	}
	select {
	case e := <-w.Updates():
		if e == nil || e.Key() != "k" || e.Revision() != 1 {
			t.Fatalf("expected live update k@1, got %v", e)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("live update not delivered")
	}
}

// TestKVSnapshotBoundaryFilter: when the filter excludes the pinned last
// sequence, the marker must still be emitted (delta==0 fallback) right after
// the last matching snapshot entry, and updates to the filtered keys are live.
func TestKVSnapshotBoundaryFilter(t *testing.T) {
	ctx := context.Background()
	js := jsForTest(t)
	kv, err := js.CreateKeyValue(ctx, KeyValueConfig{Bucket: "FILT"})
	if err != nil {
		t.Fatalf("CreateKeyValue: %v", err)
	}
	if _, err := kv.PutString(ctx, "a.1", "a1"); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if _, err := kv.PutString(ctx, "b.1", "b1"); err != nil {
		t.Fatalf("Put: %v", err)
	}

	w, err := kv.Watch(ctx, "a.>", SnapshotBoundary())
	if err != nil {
		t.Fatalf("Watch: %v", err)
	}
	defer w.Stop()

	d := drainWatcher(t, w, time.Second)
	if len(d.snapshot) != 1 {
		t.Fatalf("filtered snapshot entries = %d, want 1", len(d.snapshot))
	}
	if d.snapshot[0].Key() != "a.1" || string(d.snapshot[0].Value()) != "a1" {
		t.Fatalf("unexpected snapshot entry: key=%q value=%q", d.snapshot[0].Key(), d.snapshot[0].Value())
	}
	if d.snapshot[0].Revision() > 2 {
		t.Fatalf("snapshot revision %d above boundary 2", d.snapshot[0].Revision())
	}

	// Updates to the filtered subject arrive live; other subjects are ignored.
	if _, err := kv.PutString(ctx, "b.2", "b2"); err != nil {
		t.Fatalf("Put b.2: %v", err)
	}
	if _, err := kv.PutString(ctx, "a.2", "a2"); err != nil {
		t.Fatalf("Put a.2: %v", err)
	}
	select {
	case e := <-w.Updates():
		if e == nil || e.Key() != "a.2" || string(e.Value()) != "a2" {
			t.Fatalf("expected live a.2, got %v", e)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("live update on filtered subject not delivered")
	}
}

// TestKVSnapshotBoundaryIncludeHistory: with IncludeHistory the snapshot
// contains every revision up to the pinned sequence and the marker sits
// exactly at the boundary before live revisions.
func TestKVSnapshotBoundaryIncludeHistory(t *testing.T) {
	ctx := context.Background()
	js := jsForTest(t)
	kv, err := js.CreateKeyValue(ctx, KeyValueConfig{Bucket: "HIST", History: 10})
	if err != nil {
		t.Fatalf("CreateKeyValue: %v", err)
	}
	const writes = 5
	for i := 0; i < writes; i++ {
		if _, err := kv.PutString(ctx, "k", fmt.Sprintf("v%d", i)); err != nil {
			t.Fatalf("Put: %v", err)
		}
	}

	w, err := kv.WatchAll(ctx, SnapshotBoundary(), IncludeHistory())
	if err != nil {
		t.Fatalf("WatchAll: %v", err)
	}
	defer w.Stop()

	d := drainWatcher(t, w, time.Second)
	if len(d.snapshot) != writes {
		t.Fatalf("history snapshot entries = %d, want %d", len(d.snapshot), writes)
	}
	for i, e := range d.snapshot {
		if e.Revision() != uint64(i+1) {
			t.Fatalf("history snapshot[%d] revision = %d, want %d", i, e.Revision(), i+1)
		}
		if string(e.Value()) != fmt.Sprintf("v%d", i) {
			t.Fatalf("history snapshot[%d] value = %q, want v%d", i, e.Value(), i)
		}
	}
	if len(d.live) != 0 {
		t.Fatalf("unexpected live entries: %d", len(d.live))
	}

	if _, err := kv.PutString(ctx, "k", "vLive"); err != nil {
		t.Fatalf("Put live: %v", err)
	}
	select {
	case e := <-w.Updates():
		if e == nil || e.Revision() != writes+1 || string(e.Value()) != "vLive" {
			t.Fatalf("expected live revision %d/vLive, got %v", writes+1, e)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("live history update not delivered")
	}
}
