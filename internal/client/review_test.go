package client

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Paulkm2006/p2pfs/internal/codec"
	"github.com/Paulkm2006/p2pfs/internal/model"
)

func reviewFixture(t *testing.T, handler http.HandlerFunc) (*Client, model.Manifest, string) {
	t.Helper()
	if handler == nil {
		handler = func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) }
	}
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	c, err := New(Config{ServerURL: server.URL, DataDir: t.TempDir(), InsecureLocal: true})
	if err != nil {
		t.Fatal(err)
	}
	source := filepath.Join(t.TempDir(), "original.bin")
	if err := os.WriteFile(source, []byte("original data must survive unsharing"), 0600); err != nil {
		t.Fatal(err)
	}
	m, err := codec.Encode(context.Background(), source, c.cacheDir())
	if err != nil {
		t.Fatal(err)
	}
	c.state.Manifests[m.ID] = m
	c.state.Sharing[m.ID] = true
	return c, m, source
}

func TestReviewUnshareSerializesHoldings(t *testing.T) {
	firstStarted := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(release) })
	var requests atomic.Int32
	bodies := make(chan model.HoldingsRequest, 2)
	c, m, _ := reviewFixture(t, func(w http.ResponseWriter, r *http.Request) {
		var body model.HoldingsRequest
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, err.Error(), 400)
			return
		}
		if requests.Add(1) == 1 {
			close(firstStarted)
			<-release
		}
		bodies <- body
		w.WriteHeader(http.StatusNoContent)
	})
	reportDone := make(chan error, 1)
	go func() { reportDone <- c.report(context.Background(), m) }()
	select {
	case <-firstStarted:
	case <-time.After(3 * time.Second):
		t.Fatal("initial holdings request did not start")
	}
	unshareDone := make(chan error, 1)
	go func() { unshareDone <- c.Unshare(context.Background(), m.ID, false) }()
	deadline := time.Now().Add(3 * time.Second)
	for {
		c.mu.Lock()
		shared := c.state.Sharing[m.ID]
		c.mu.Unlock()
		if !shared {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("Unshare did not disable local serving")
		}
		time.Sleep(time.Millisecond)
	}
	releaseOnce.Do(func() { close(release) })
	for _, done := range []chan error{reportDone, unshareDone} {
		select {
		case err := <-done:
			if err != nil {
				t.Fatal(err)
			}
		case <-time.After(3 * time.Second):
			t.Fatal("holdings operation did not finish")
		}
	}
	first, last := <-bodies, <-bodies
	if len(first.Indices) != model.TotalShards || len(last.Indices) != 0 {
		t.Fatalf("holdings order re-advertised a stopped file: first=%v last=%v", first.Indices, last.Indices)
	}
}

func TestReviewUnshareWaitsForCanceledWorker(t *testing.T) {
	c, m, source := reviewFixture(t, nil)
	c.state.Tasks["task"] = &Task{ID: "task", FileID: m.ID, State: "cancelled"}
	c.runs["task"] = &runningTask{cancel: func() {}, done: make(chan struct{})}
	if err := c.Unshare(context.Background(), m.ID, true); err == nil {
		t.Fatal("deleted cache while canceled worker was still running")
	}
	if len(codec.Existing(m, c.cacheDir())) != model.TotalShards {
		t.Fatal("live-worker rejection deleted shards")
	}
	delete(c.runs, "task")
	if err := c.Unshare(context.Background(), m.ID, true); err != nil {
		t.Fatal(err)
	}
	if len(codec.Existing(m, c.cacheDir())) != 0 {
		t.Fatal("cache remained after completed worker unshared")
	}
	got, err := os.ReadFile(source)
	if err != nil || !bytes.Equal(got, []byte("original data must survive unsharing")) {
		t.Fatalf("original file changed: %v", err)
	}
}

func TestReviewUnshareRejectsBusyFile(t *testing.T) {
	c, m, _ := reviewFixture(t, nil)
	if err := c.beginOperation(m.ID); err != nil {
		t.Fatal(err)
	}
	defer c.endOperation(m.ID)
	if err := c.Unshare(context.Background(), m.ID, true); err == nil {
		t.Fatal("unshared a file owned by another cache operation")
	}
	if len(codec.Existing(m, c.cacheDir())) != model.TotalShards {
		t.Fatal("operation-guard rejection deleted shards")
	}
}

func TestReviewUnshareDoesNotFollowCacheDirectorySymlink(t *testing.T) {
	c, m, _ := reviewFixture(t, nil)
	dir := filepath.Join(c.cacheDir(), m.ID)
	if err := os.Rename(dir, dir+"-original"); err != nil {
		t.Fatal(err)
	}
	outside := t.TempDir()
	sentinel := filepath.Join(outside, "0.shard")
	expected := []byte("unrelated user file")
	if err := os.WriteFile(sentinel, expected, 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, dir); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}
	if err := c.Unshare(context.Background(), m.ID, true); err == nil {
		t.Fatal("Unshare should reject a symlinked cache version directory")
	}
	got, err := os.ReadFile(sentinel)
	if err != nil || !bytes.Equal(got, expected) {
		t.Fatalf("Unshare modified a file outside its cache: %v", err)
	}
}
