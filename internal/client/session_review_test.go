package client

import (
	"context"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Paulkm2006/p2pfs/internal/coordinator"
)

func TestSessionRecoveryRetriesTransientHoldingsFailure(t *testing.T) {
	s, err := coordinator.New(coordinator.Config{DatabasePath: filepath.Join(t.TempDir(), "server.db")})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	var failNextHoldings atomic.Bool
	h := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/holdings" && failNextHoldings.CompareAndSwap(true, false) {
			http.Error(w, "temporary unavailable", http.StatusServiceUnavailable)
			return
		}
		s.Handler().ServeHTTP(w, r)
	}))
	defer h.Close()
	c, err := New(Config{ServerURL: h.URL, InsecureLocal: true, DataDir: t.TempDir(), TransportOptions: testOptions(t)})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if err = c.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	path, _ := sampleFile(t, 4096)
	m, err := c.Publish(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	id := c.Identity().UUID
	failNextHoldings.Store(true)
	if err = c.register(context.Background()); err == nil {
		t.Fatal("injected holdings failure was not observed")
	}
	if c.Identity().UUID != id {
		t.Fatal("recovery changed the persistent device identity")
	}
	info, err := c.file(context.Background(), m.ID)
	if err != nil || len(info.Sources) != 0 {
		t.Fatalf("old session claims were not invalidated: %+v, %v", info.Sources, err)
	}
	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		info, err = c.file(context.Background(), m.ID)
		if err == nil && len(info.Sources) == 1 && info.Sources[0].PeerID == id && len(info.Sources[0].Indices) == 6 {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("valid new session never retried holdings after transient recovery error: %+v, %v", info.Sources, err)
}
