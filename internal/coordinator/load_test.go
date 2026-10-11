package coordinator

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Paulkm2006/p2pfs/internal/codec"
	"github.com/Paulkm2006/p2pfs/internal/model"
)

const controlLoadDuration = 10 * time.Minute

// TestControlPlaneLoadAcceptance is deliberately opt-in and has no shortened
// duration override: NFR-003 requires a continuous ten-minute measured interval.
// The main open-loop mix is 10 requests/second. Another 10 heartbeats/second
// refresh all 50 peers every five seconds using unchanged production defaults.
func TestControlPlaneLoadAcceptance(t *testing.T) {
	if os.Getenv("P2PFS_CONTROL_LOAD") != "1" {
		t.Skip("set P2PFS_CONTROL_LOAD=1 to run the full ten-minute NFR-003 test")
	}
	f := newControlLoadFixture(t)
	t.Logf("environment: go=%s os=%s arch=%s logical_cpus=%d gomaxprocs=%d database=%s", runtime.Version(), runtime.GOOS, runtime.GOARCH, runtime.NumCPU(), runtime.GOMAXPROCS(0), f.s.cfg.DatabasePath)
	for _, path := range []string{"/proc/meminfo", "/sys/fs/cgroup/cpu.max", "/sys/fs/cgroup/memory.max"} {
		b, err := os.ReadFile(path)
		if err != nil {
			t.Logf("environment: %s unavailable: %v", path, err)
		} else {
			t.Logf("environment: %s: %s", path, strings.SplitN(string(b), "\n", 2)[0])
		}
	}
	for _, p := range f.peers {
		if err := f.call("", "POST", "/v1/heartbeat", p.Token, nil, 200); err != nil {
			t.Fatal(err)
		}
	}
	f.assertCounts(t)

	var wg sync.WaitGroup
	var resultMu sync.Mutex
	var failures []string
	var failedWrites, errorsTotal int
	var maxLag, maxPeerAge time.Duration
	minOnline := len(f.peers)
	start := time.Now()
	end := start.Add(controlLoadDuration)
	t.Logf("measurement_start_utc=%s duration=%s main_rps=10 maintenance_heartbeat_rps=10", start.UTC().Format(time.RFC3339Nano), controlLoadDuration)
	// Fixed intended arrival times avoid a closed-loop client hiding slow work.
	// Each half-slot alternates one main request with one maintenance heartbeat.
	for slot := 0; slot < int(controlLoadDuration/(50*time.Millisecond)); slot++ {
		due := start.Add(time.Duration(slot) * 50 * time.Millisecond)
		time.Sleep(time.Until(due))
		if lag := time.Since(due); lag > maxLag {
			maxLag = lag
		}
		n := slot / 2
		maintenance := slot%2 == 1
		wg.Add(1)
		go func() {
			defer wg.Done()
			var err error
			var write bool
			if maintenance {
				err = f.call("maintenance", "POST", "/v1/heartbeat", f.peers[n%50].Token, nil, 200)
			} else {
				err, write = f.mixed(n, "main")
			}
			if err != nil {
				resultMu.Lock()
				errorsTotal++
				if write {
					failedWrites++
				}
				if len(failures) < 20 {
					failures = append(failures, err.Error())
				}
				resultMu.Unlock()
			}
		}()
		if slot%20 == 0 {
			f.s.mu.Lock()
			now := time.Now()
			online := 0
			for _, peer := range f.peers {
				p := f.s.peers[peer.UUID]
				if p != nil && now.Before(p.expires) && now.Sub(p.lastSeen) < f.s.cfg.OnlineTTL {
					online++
				}
				if p != nil && now.Sub(p.lastSeen) > maxPeerAge {
					maxPeerAge = now.Sub(p.lastSeen)
				}
			}
			f.s.mu.Unlock()
			if online < minOnline {
				minOnline = online
			}
		}
		if slot > 0 && slot%1200 == 0 {
			t.Logf("progress: elapsed=%s main_scheduled=%d maintenance_scheduled=%d min_online=%d", time.Since(start).Round(time.Millisecond), n+1, n, minOnline)
		}
	}
	time.Sleep(time.Until(end))
	wg.Wait()
	// Close waits for every HTTP handler, including the timing wrapper, to exit.
	f.http.Close()
	elapsed := time.Since(start)
	f.assertCounts(t)
	f.stats.mu.Lock()
	defer f.stats.mu.Unlock()
	allMain := []time.Duration{}
	all := []time.Duration{}
	keys := make([]string, 0, len(f.stats.durations))
	for key := range f.stats.durations {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		d := f.stats.durations[key]
		all = append(all, d...)
		if strings.HasPrefix(key, "main ") {
			allMain = append(allMain, d...)
		}
		t.Logf("server %s: requests=%d p50=%s p95=%s p99=%s max=%s statuses=%v", key, len(d), loadPercentile(d, .50), loadPercentile(d, .95), loadPercentile(d, .99), loadPercentile(d, 1), f.stats.statuses[key])
	}
	p95 := loadPercentile(allMain, .95)
	t.Logf("measurement_end_utc=%s elapsed=%s main_requests=%d maintenance_requests=%d main_achieved_rps=%.6f", time.Now().UTC().Format(time.RFC3339Nano), elapsed, len(allMain), len(all)-len(allMain), float64(len(allMain))/elapsed.Seconds())
	t.Logf("summary: main_server_p95=%s combined_server_p95=%s errors=%d unrecovered_write_errors=%d min_online_peers=%d max_observed_heartbeat_age=%s max_schedule_lag=%s", p95, loadPercentile(all, .95), errorsTotal, failedWrites, minOnline, maxPeerAge, maxLag)
	if len(allMain) != 6000 || len(all) != 12000 || elapsed < controlLoadDuration || elapsed > controlLoadDuration+time.Second {
		t.Errorf("target rate/duration not sustained: main=%d total=%d elapsed=%s", len(allMain), len(all), elapsed)
	}
	if maxLag >= 100*time.Millisecond {
		t.Errorf("open-loop scheduler missed a complete 100 ms main-workload slot: %s", maxLag)
	}
	if p95 > 500*time.Millisecond || loadPercentile(all, .95) > 500*time.Millisecond {
		t.Errorf("server p95 exceeded 500 ms")
	}
	if minOnline != 50 || errorsTotal != 0 || failedWrites != 0 {
		t.Errorf("load correctness failed: online=%d errors=%d write_errors=%d first_errors=%v", minOnline, errorsTotal, failedWrites, failures)
	}
	var integrity string
	if err := f.s.db.QueryRow("PRAGMA integrity_check").Scan(&integrity); err != nil || integrity != "ok" {
		t.Errorf("SQLite integrity_check=%q err=%v", integrity, err)
	}
	t.Logf("SQLite integrity_check=%s", integrity)
}

// A regular, fast test checks the exact fixture and each request in the mix.
// It is not a replacement for TestControlPlaneLoadAcceptance.
func TestControlPlaneLoadFixture(t *testing.T) {
	f := newControlLoadFixture(t)
	for n := range 10 {
		if err, _ := f.mixed(n, ""); err != nil {
			t.Fatal(err)
		}
	}
	f.assertCounts(t)
}

type controlLoadFixture struct {
	s         *Server
	http      *httptest.Server
	client    *http.Client
	peers     []model.RegisterResponse
	manifests []model.Manifest
	stats     *controlLoadStats
}

type controlLoadStats struct {
	mu        sync.Mutex
	durations map[string][]time.Duration
	statuses  map[string]map[int]int
}

type controlLoadWriter struct {
	http.ResponseWriter
	status int
}

func (w *controlLoadWriter) WriteHeader(status int) {
	if w.status == 0 {
		w.status = status
	}
	w.ResponseWriter.WriteHeader(status)
}
func (w *controlLoadWriter) Write(b []byte) (int, error) {
	if w.status == 0 {
		w.status = http.StatusOK
	}
	return w.ResponseWriter.Write(b)
}
func (w *controlLoadWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }

func newControlLoadFixture(t *testing.T) *controlLoadFixture {
	t.Helper()
	f := &controlLoadFixture{s: newServer(t, Config{}), stats: &controlLoadStats{durations: map[string][]time.Duration{}, statuses: map[string]map[int]int{}}}
	f.http = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		stream := r.Header.Get("X-P2PFS-Load-Stream")
		if stream == "" {
			f.s.Handler().ServeHTTP(w, r)
			return
		}
		path := r.URL.Path
		if strings.HasPrefix(path, "/v1/files/") {
			path = "/v1/files/:id"
		}
		key := stream + " " + r.Method + " " + path
		rw := &controlLoadWriter{ResponseWriter: w}
		start := time.Now()
		f.s.Handler().ServeHTTP(rw, r)
		duration := time.Since(start)
		f.stats.mu.Lock()
		f.stats.durations[key] = append(f.stats.durations[key], duration)
		if f.stats.statuses[key] == nil {
			f.stats.statuses[key] = map[int]int{}
		}
		f.stats.statuses[key][rw.status]++
		f.stats.mu.Unlock()
	}))
	t.Cleanup(f.http.Close)
	transport := &http.Transport{MaxIdleConns: 100, MaxIdleConnsPerHost: 100, MaxConnsPerHost: 100}
	t.Cleanup(transport.CloseIdleConnections)
	f.client = &http.Client{Transport: transport, Timeout: 5 * time.Second}
	for i := range 50 {
		body, _ := json.Marshal(model.RegisterRequest{Name: fmt.Sprintf("synthetic-load-peer-%02d", i)})
		resp, err := f.client.Post(f.http.URL+"/v1/register", "application/json", bytes.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		var p model.RegisterResponse
		err = json.NewDecoder(resp.Body).Decode(&p)
		resp.Body.Close()
		if err != nil || resp.StatusCode != 200 || len(p.UUID) != 36 || p.Token == "" {
			t.Fatalf("register fixture peer: status=%d err=%v", resp.StatusCode, err)
		}
		f.peers = append(f.peers, p)
	}
	// Encode synthetic content once; the 500 distinct, valid named manifests
	// describe the same tiny deterministic payload. No real user file is read.
	dir := t.TempDir()
	input := filepath.Join(dir, "synthetic.bin")
	if err := os.WriteFile(input, []byte{0x13, 0x27, 0x42, 0x99}, 0600); err != nil {
		t.Fatal(err)
	}
	base, err := codec.Encode(context.Background(), input, filepath.Join(dir, "shards"))
	if err != nil {
		t.Fatal(err)
	}
	// Direct, same-package DB fixture avoids burning production API rate limits
	// during setup. Measured requests all use actual HTTP/Echo and SQLite.
	tx, err := f.s.db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	for i := range 500 {
		m := base
		m.Name = fmt.Sprintf("synthetic-load-%03d.bin", i)
		m.ID = codec.ManifestID(m)
		if err := codec.ValidateManifest(m); err != nil {
			t.Fatal(err)
		}
		body, _ := json.Marshal(m)
		if _, err := tx.Exec("INSERT INTO manifests(id,body) VALUES(?,?)", m.ID, body); err != nil {
			t.Fatal(err)
		}
		for pair := range 2 {
			p := f.peers[(i+pair)%50]
			for shard := range 5 {
				if _, err := tx.Exec("INSERT INTO holdings(file_id,peer_id,shard_index,session_hash) VALUES(?,?,?,?)", m.ID, p.UUID, shard, digest(p.Token)); err != nil {
					t.Fatal(err)
				}
			}
		}
		f.manifests = append(f.manifests, m)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	f.assertCounts(t)
	return f
}

func (f *controlLoadFixture) assertCounts(t *testing.T) {
	t.Helper()
	for table, want := range map[string]int{"devices": 50, "manifests": 500, "holdings": 5000} {
		var got int
		if err := f.s.db.QueryRow("SELECT COUNT(*) FROM " + table).Scan(&got); err != nil || got != want {
			t.Fatalf("fixture %s count=%d want=%d err=%v", table, got, want, err)
		}
	}
}

func (f *controlLoadFixture) mixed(n int, stream string) (error, bool) {
	file := (n*17 + n/10) % 500
	m := f.manifests[file]
	peer := f.peers[n%50]
	switch n % 10 {
	case 0, 5:
		return f.call(stream, "GET", "/v1/files", peer.Token, nil, 200), false
	case 1, 4, 8:
		return f.call(stream, "GET", "/v1/files/"+m.ID, peer.Token, nil, 200), false
	case 2, 6, 9:
		// Alternate the omitted shard while keeping exactly five rows per pair.
		indices := []int{0, 1, 2, 3, 4}
		if (n/10)%2 == 1 {
			indices = []int{1, 2, 3, 4, 5}
		}
		peer = f.peers[(file+(n/10)%2)%50]
		return f.call(stream, "PUT", "/v1/holdings", peer.Token, model.HoldingsRequest{FileID: m.ID, Indices: indices}, 204), true
	case 3:
		// Immutable, idempotent re-publication; holdings above are the actual
		// DELETE/INSERT/COMMIT writes. Cardinalities stay exactly 500/5,000.
		return f.call(stream, "POST", "/v1/manifests", peer.Token, m, 204), false
	default:
		return f.call(stream, "POST", "/v1/heartbeat", peer.Token, nil, 200), false
	}
}

func (f *controlLoadFixture) call(stream, method, path, token string, body any, want int) error {
	var data []byte
	if body != nil {
		var err error
		data, err = json.Marshal(body)
		if err != nil {
			return err
		}
	}
	req, err := http.NewRequest(method, f.http.URL+path, bytes.NewReader(data))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("X-P2PFS-Load-Stream", stream)
	resp, err := f.client.Do(req)
	if err != nil {
		return fmt.Errorf("%s %s: %w", method, path, err)
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("%s %s response: %w", method, path, err)
	}
	if resp.StatusCode != want {
		return fmt.Errorf("%s %s: got=%d want=%d", method, path, resp.StatusCode, want)
	}
	if method == "GET" {
		var files []model.FileInfo
		if path == "/v1/files" {
			if err := json.Unmarshal(b, &files); err != nil || len(files) != 500 {
				return fmt.Errorf("file-list count=%d err=%v", len(files), err)
			}
		} else {
			var file model.FileInfo
			if err := json.Unmarshal(b, &file); err != nil {
				return err
			}
			if "/v1/files/"+file.Manifest.ID != path {
				return fmt.Errorf("detail response returned another manifest")
			}
			files = append(files, file)
		}
		for _, file := range files {
			if len(file.Sources) != 2 || len(file.Sources[0].Indices) != 5 || len(file.Sources[1].Indices) != 5 {
				return fmt.Errorf("file %s: expected two online peers with five holdings each", file.Manifest.ID)
			}
		}
	}
	return nil
}

func loadPercentile(in []time.Duration, quantile float64) time.Duration {
	if len(in) == 0 {
		return 0
	}
	values := append([]time.Duration(nil), in...)
	sort.Slice(values, func(i, j int) bool { return values[i] < values[j] })
	return values[int(math.Ceil(float64(len(values))*quantile))-1]
}
