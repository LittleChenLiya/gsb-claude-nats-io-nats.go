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
	"testing"
	"time"
)

// TestKVDefaultWatcherUnchanged pins the pre-existing default Watch
// semantics: one nil marker after the initial/latest values, then live
// updates afterwards. This must keep passing unchanged when
// SnapshotBoundary is not used.
func TestKVDefaultWatcherUnchanged(t *testing.T) {
	ctx := context.Background()
	js := jsForTest(t)
	kv, err := js.CreateKeyValue(ctx, KeyValueConfig{Bucket: "DEF", History: 2})
	if err != nil {
		t.Fatalf("CreateKeyValue: %v", err)
	}

	// Empty bucket: marker immediately.
	w0, err := kv.WatchAll(ctx)
	if err != nil {
		t.Fatalf("WatchAll: %v", err)
	}
	select {
	case e := <-w0.Updates():
		if e != nil {
			t.Fatalf("empty bucket: expected nil marker, got %v", e)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("empty bucket: marker not received")
	}
	w0.Stop()

	// Seed data and watch again: latest values then marker.
	for i := 0; i < 3; i++ {
		if _, err := kv.PutString(ctx, fmt.Sprintf("k%d", i), "v1"); err != nil {
			t.Fatalf("Put: %v", err)
		}
	}
	w, err := kv.WatchAll(ctx)
	if err != nil {
		t.Fatalf("WatchAll: %v", err)
	}
	defer w.Stop()

	got := 0
	for range 3 {
		select {
		case e := <-w.Updates():
			if e == nil {
				t.Fatalf("marker too early after %d entries", got)
			}
			got++
		case <-time.After(2 * time.Second):
			t.Fatalf("missing initial value, got %d", got)
		}
	}
	select {
	case e := <-w.Updates():
		if e != nil {
			t.Fatalf("expected nil marker, got %v", e)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("marker not received")
	}

	// Live update after marker.
	if _, err := kv.PutString(ctx, "live", "x"); err != nil {
		t.Fatalf("Put live: %v", err)
	}
	select {
	case e := <-w.Updates():
		if e == nil || e.Key() != "live" {
			t.Fatalf("expected live entry, got %v", e)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("live update not received")
	}

	// UpdatesOnly: no marker, no initial values — only live updates.
	wu, err := kv.WatchAll(ctx, UpdatesOnly())
	if err != nil {
		t.Fatalf("WatchAll updatesOnly: %v", err)
	}
	defer wu.Stop()
	select {
	case e := <-wu.Updates():
		t.Fatalf("UpdatesOnly delivered an entry before any write: %v", e)
	case <-time.After(150 * time.Millisecond):
	}
	if _, err := kv.PutString(ctx, "uo-live", "y"); err != nil {
		t.Fatalf("Put: %v", err)
	}
	select {
	case e := <-wu.Updates():
		if e == nil || e.Key() != "uo-live" {
			t.Fatalf("expected uo-live entry, got %v", e)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("UpdatesOnly live entry not received")
	}
}
