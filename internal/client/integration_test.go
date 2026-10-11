package client

import (
	"bytes"
	"context"
	"encoding/json"
	"github.com/Paulkm2006/p2pfs/internal/transport"
	"github.com/Paulkm2006/p2pfs/internal/transport/testnet"
	"github.com/pion/transport/v4/stdnet"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Paulkm2006/p2pfs/internal/codec"
	"github.com/Paulkm2006/p2pfs/internal/coordinator"
	"github.com/Paulkm2006/p2pfs/internal/model"
)

func testServer(t *testing.T) (string, *coordinator.Server) {
	t.Helper()
	s, e := coordinator.New(coordinator.Config{DatabasePath: filepath.Join(t.TempDir(), "server.db")})
	if e != nil {
		t.Fatal(e)
	}
	h := httptest.NewServer(s.Handler())
	t.Cleanup(func() { h.Close(); s.Close() })
	return h.URL, s
}
func testPeer(t *testing.T, url string) *Client {
	t.Helper()
	c, e := New(Config{ServerURL: url, DataDir: t.TempDir(), InsecureLocal: true, Name: t.Name(), TransportOptions: testOptions(t)})
	if e != nil {
		t.Fatal(e)
	}
	if e = c.Start(context.Background()); e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { c.Close() })
	return c
}
func sampleFile(t *testing.T, size int) (string, []byte) {
	t.Helper()
	data := make([]byte, size)
	for i := range data {
		data[i] = byte((i*31 + i/7) % 251)
	}
	path := filepath.Join(t.TempDir(), "sample.bin")
	if e := os.WriteFile(path, data, 0600); e != nil {
		t.Fatal(e)
	}
	return path, data
}
func waitTask(t *testing.T, c *Client, id, state string) Task {
	t.Helper()
	deadline := time.After(45 * time.Second)
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		for _, task := range c.Tasks() {
			if task.ID != id {
				continue
			}
			if task.State == state {
				return task
			}
			if task.State == "failed" {
				t.Fatalf("task failed: %s", task.Error)
			}
		}
		select {
		case <-deadline:
			t.Fatalf("timeout waiting for %s: %+v", state, c.Tasks())
		case <-ticker.C:
		}
	}
}
func TestMultiPeerDistributionPublisherOffline(t *testing.T) {
	url, _ := testServer(t)
	publisher := testPeer(t, url)
	path, want := sampleFile(t, 2<<20)
	m, e := publisher.Publish(context.Background(), path)
	if e != nil {
		t.Fatal(e)
	}
	holders := []*Client{testPeer(t, url), testPeer(t, url), testPeer(t, url)}
	for i, h := range holders {
		if e = h.Retain(context.Background(), m.ID, []int{2 * i, 2*i + 1}); e != nil {
			t.Fatal(e)
		}
	}
	// Explicitly withdraw publisher holdings; closing alone expires via heartbeat.
	if e = publisher.Unshare(context.Background(), m.ID, false); e != nil {
		t.Fatal(e)
	}
	publisher.Close()
	downloader := testPeer(t, url)
	output := filepath.Join(t.TempDir(), "result.bin")
	id, e := downloader.StartDownload(m.ID, output)
	if e != nil {
		t.Fatal(e)
	}
	task := waitTask(t, downloader, id, "complete")
	got, e := os.ReadFile(output)
	if e != nil || !bytes.Equal(got, want) {
		t.Fatalf("reconstruction mismatch: %v", e)
	}
	peers := map[string]bool{}
	for _, tr := range task.Transfers {
		if tr.Error == "" {
			peers[tr.PeerID] = true
			if tr.Path != "direct" {
				t.Errorf("expected actual direct candidate, got %q", tr.Path)
			}
		}
	}
	if len(peers) < 2 {
		t.Fatalf("not multi-source: %+v", task.Transfers)
	}
	overlap := false
	for i, a := range task.Transfers {
		for j, b := range task.Transfers {
			if i != j && a.PeerID != b.PeerID && a.StartedAt.Before(b.StartedAt.Add(time.Duration(b.Milliseconds)*time.Millisecond)) && b.StartedAt.Before(a.StartedAt.Add(time.Duration(a.Milliseconds)*time.Millisecond)) {
				overlap = true
			}
		}
	}
	if !overlap {
		t.Fatal("different sources did not overlap")
	}
}
func TestPersistentUUIDAndPausedTaskRecovery(t *testing.T) {
	url, _ := testServer(t)
	publisher := testPeer(t, url)
	path, want := sampleFile(t, 128<<10)
	m, e := publisher.Publish(context.Background(), path)
	if e != nil {
		t.Fatal(e)
	}
	dir := t.TempDir()
	cfg := Config{ServerURL: url, DataDir: dir, InsecureLocal: true, TransportOptions: testOptions(t)}
	c, e := New(cfg)
	if e != nil {
		t.Fatal(e)
	}
	if e = c.Start(context.Background()); e != nil {
		t.Fatal(e)
	}
	uuid := c.Identity().UUID
	if e = c.Retain(context.Background(), m.ID, []int{0, 1}); e != nil {
		t.Fatal(e)
	}
	output := filepath.Join(t.TempDir(), "restored.bin")
	id, e := c.StartDownload(m.ID, output)
	if e != nil {
		t.Fatal(e)
	}
	if e = c.Pause(id); e != nil {
		t.Fatal(e)
	}
	if e = c.Close(); e != nil {
		t.Fatal(e)
	}
	c2, e := New(cfg)
	if e != nil {
		t.Fatal(e)
	}
	if e = c2.Start(context.Background()); e != nil {
		t.Fatal(e)
	}
	defer c2.Close()
	if c2.Identity().UUID != uuid {
		t.Fatal("UUID changed across restart")
	}
	if c2.Tasks()[0].State != "paused" {
		t.Fatalf("restore state: %+v", c2.Tasks())
	}
	if e = c2.Resume(id); e != nil {
		t.Fatal(e)
	}
	task := waitTask(t, c2, id, "complete")
	for _, tr := range task.Transfers {
		if tr.Index == 0 || tr.Index == 1 {
			t.Fatal("redownloaded verified complete shard")
		}
	}
	got, e := os.ReadFile(output)
	if e != nil || !bytes.Equal(got, want) {
		t.Fatalf("reconstruction mismatch %v", e)
	}
	raw, e := os.ReadFile(filepath.Join(dir, "config.json"))
	if e != nil {
		t.Fatal(e)
	}
	var saved localState
	if e = json.Unmarshal(raw, &saved); e != nil {
		t.Fatal(e)
	}
	if saved.Identity.DeviceSecret == "" || saved.Identity.UUID != uuid {
		t.Fatal("identity was not persisted")
	}
}
func TestWaitForSourcesAndCancel(t *testing.T) {
	url, _ := testServer(t)
	p := testPeer(t, url)
	path, _ := sampleFile(t, 4096)
	m, e := p.Publish(context.Background(), path)
	if e != nil {
		t.Fatal(e)
	}
	if e = p.Unshare(context.Background(), m.ID, false); e != nil {
		t.Fatal(e)
	}
	d := testPeer(t, url)
	id, e := d.StartDownload(m.ID, filepath.Join(t.TempDir(), "wait.bin"))
	if e != nil {
		t.Fatal(e)
	}
	waitTask(t, d, id, "waiting")
	if e = d.Cancel(id); e != nil {
		t.Fatal(e)
	}
	waitTask(t, d, id, "cancelled")
}
func TestCorruptCacheNotReused(t *testing.T) {
	url, _ := testServer(t)
	p := testPeer(t, url)
	path, want := sampleFile(t, 32000)
	m, e := p.Publish(context.Background(), path)
	if e != nil {
		t.Fatal(e)
	}
	d := testPeer(t, url)
	if e = d.Retain(context.Background(), m.ID, []int{0, 1}); e != nil {
		t.Fatal(e)
	}
	if e = os.WriteFile(codec.ShardPath(d.cacheDir(), m.ID, 0), []byte("bad"), 0600); e != nil {
		t.Fatal(e)
	}
	dest := filepath.Join(t.TempDir(), "ok.bin")
	id, e := d.StartDownload(m.ID, dest)
	if e != nil {
		t.Fatal(e)
	}
	waitTask(t, d, id, "complete")
	got, e := os.ReadFile(dest)
	if e != nil || !bytes.Equal(got, want) {
		t.Fatal("corrupt cache was accepted")
	}
}
func TestSecurityLocalConfiguration(t *testing.T) {
	for _, u := range []string{"http://example.com", "http://127.0.0.1", "ftp://localhost", "https://user:secret@example.com"} {
		if _, e := New(Config{ServerURL: u, DataDir: t.TempDir()}); e == nil {
			t.Errorf("accepted unsafe URL %s", u)
		}
	}
}
func TestSchedulerPrefersDiverseHealthySources(t *testing.T) {
	c := &Client{cfg: Config{}, state: localState{Identity: model.Identity{UUID: "self"}}, scores: map[string]peerScore{"bad": {failures: 2, failedAt: time.Now()}}}
	f := model.FileInfo{Sources: []model.Source{{PeerID: "bad", Indices: []int{0, 1, 2}}, {PeerID: "good", Indices: []int{0, 1}}, {PeerID: "rare", Indices: []int{3}}}}
	got := c.choose(f, map[int]bool{}, 3)
	if len(got) != 3 || got[0].index != 2 && got[0].index != 3 {
		t.Fatalf("rarity ignored: %+v", got)
	}
}

func TestCoordinatorBoundUDPRelay(t *testing.T) {
	s, e := coordinator.New(coordinator.Config{DatabasePath: filepath.Join(t.TempDir(), "relay.db"), TURN: &coordinator.TURNConfig{ListenAddress: "127.0.0.1:0", PublicIP: "127.0.0.1", AllowLoopback: true, Network: &stdnet.Net{}}})
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	h := httptest.NewServer(s.Handler())
	defer h.Close()
	makePeer := func() *Client {
		c, e := New(Config{ServerURL: h.URL, DataDir: t.TempDir(), InsecureLocal: true, ForceRelay: true, TransportOptions: testOptions(t)})
		if e != nil {
			t.Fatal(e)
		}
		if e = c.Start(context.Background()); e != nil {
			t.Fatal(e)
		}
		t.Cleanup(func() { c.Close() })
		return c
	}
	p, d := makePeer(), makePeer()
	path, want := sampleFile(t, 256<<10)
	m, e := p.Publish(context.Background(), path)
	if e != nil {
		t.Fatal(e)
	}
	dest := filepath.Join(t.TempDir(), "relay.bin")
	id, e := d.StartDownload(m.ID, dest)
	if e != nil {
		t.Fatal(e)
	}
	task := waitTask(t, d, id, "complete")
	for _, tr := range task.Transfers {
		if tr.Error == "" && tr.Path != "relay" {
			t.Errorf("actual selected path %q, expected relay", tr.Path)
		}
	}
	got, e := os.ReadFile(dest)
	if e != nil || !bytes.Equal(got, want) {
		t.Fatalf("relay integrity: %v", e)
	}
}

func testOptions(t *testing.T) []transport.Option {
	t.Helper()
	if _, err := net.Interfaces(); err != nil {
		t.Log("explicit loopback interface fixture; real UDP sockets; host enumeration unavailable:", err)
		return []transport.Option{transport.WithNetwork(testnet.Loopback())}
	}
	return nil
}

func TestUnavailableRareShardDoesNotStarveParity(t *testing.T) {
	url, _ := testServer(t)
	p := testPeer(t, url)
	path, want := sampleFile(t, 64000)
	m, e := p.Publish(context.Background(), path)
	if e != nil {
		t.Fatal(e)
	}
	good := testPeer(t, url)
	if e = good.Retain(context.Background(), m.ID, []int{1, 2, 3, 4, 5}); e != nil {
		t.Fatal(e)
	}
	if e = p.Unshare(context.Background(), m.ID, false); e != nil {
		t.Fatal(e)
	}
	// A stale/dishonest holding claims the rare index, while its owner refuses it.
	if e = p.request(context.Background(), "PUT", "/v1/holdings", model.HoldingsRequest{FileID: m.ID, Indices: []int{0}}, nil); e != nil {
		t.Fatal(e)
	}
	d := testPeer(t, url)
	dest := filepath.Join(t.TempDir(), "parity.bin")
	id, e := d.StartDownload(m.ID, dest)
	if e != nil {
		t.Fatal(e)
	}
	waitTask(t, d, id, "complete")
	got, e := os.ReadFile(dest)
	if e != nil || !bytes.Equal(got, want) {
		t.Fatal("failed healthy parity recovery")
	}
}

func TestRegistrationNeverFollowsRedirect(t *testing.T) {
	hits := 0
	receiver := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { hits++; w.WriteHeader(200) }))
	defer receiver.Close()
	redirect := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, receiver.URL, http.StatusTemporaryRedirect)
	}))
	defer redirect.Close()
	c, e := New(Config{ServerURL: redirect.URL, DataDir: t.TempDir(), InsecureLocal: true})
	if e != nil {
		t.Fatal(e)
	}
	defer c.Close()
	if e = c.Start(context.Background()); e == nil {
		t.Fatal("redirect accepted")
	}
	if hits != 0 {
		t.Fatal("registration secret body forwarded to redirect destination")
	}
}

func TestSinglePeerBadIndexUsesHealthyParity(t *testing.T) {
	url, _ := testServer(t)
	p := testPeer(t, url)
	path, want := sampleFile(t, 32000)
	m, e := p.Publish(context.Background(), path)
	if e != nil {
		t.Fatal(e)
	}
	if e = os.Remove(codec.ShardPath(p.cacheDir(), m.ID, 0)); e != nil {
		t.Fatal(e)
	}
	d := testPeer(t, url)
	dest := filepath.Join(t.TempDir(), "parity.bin")
	id, e := d.StartDownload(m.ID, dest)
	if e != nil {
		t.Fatal(e)
	}
	waitTask(t, d, id, "complete")
	got, e := os.ReadFile(dest)
	if e != nil || !bytes.Equal(got, want) {
		t.Fatal("same peer healthy parity was starved")
	}
}
