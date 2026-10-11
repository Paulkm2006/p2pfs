package coordinator

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Paulkm2006/p2pfs/internal/codec"
	"github.com/Paulkm2006/p2pfs/internal/model"
)

func request(t *testing.T, s *Server, method, path, token string, body any, want int) *httptest.ResponseRecorder {
	t.Helper()
	var data []byte
	var err error
	if body != nil {
		data, err = json.Marshal(body)
		if err != nil {
			t.Fatal(err)
		}
	}
	r := httptest.NewRequest(method, path, bytes.NewReader(data))
	r.Header.Set("Content-Type", "application/json")
	if token != "" {
		r.Header.Set("Authorization", "Bearer "+token)
	}
	w := httptest.NewRecorder()
	s.Handler().ServeHTTP(w, r)
	if w.Code != want {
		t.Fatalf("%s %s: got %d want %d: %s", method, path, w.Code, want, w.Body.String())
	}
	return w
}
func registerPeer(t *testing.T, s *Server, id model.Identity) model.RegisterResponse {
	t.Helper()
	w := request(t, s, "POST", "/v1/register", "", model.RegisterRequest{Identity: id, Name: "test"}, 200)
	var p model.RegisterResponse
	if err := json.Unmarshal(w.Body.Bytes(), &p); err != nil {
		t.Fatal(err)
	}
	return p
}
func newServer(t *testing.T, cfg Config) *Server {
	t.Helper()
	if cfg.DatabasePath == "" {
		cfg.DatabasePath = filepath.Join(t.TempDir(), "metadata.db")
	}
	s, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Error(err)
		}
	})
	return s
}
func manifest() model.Manifest {
	sum := sha256.Sum256([]byte("test"))
	hash := hex.EncodeToString(sum[:])
	m := model.Manifest{Format: codec.Format, Name: "test.bin", Size: 4, SHA256: hash, DataShards: 4, ParityShards: 2, Codec: codec.Codec}
	for i := 0; i < 6; i++ {
		m.Shards = append(m.Shards, model.Shard{Index: i, Size: 1, SHA256: hash})
	}
	m.ID = codec.ManifestID(m)
	return m
}
func fileInfo(t *testing.T, s *Server, p model.RegisterResponse, id string) model.FileInfo {
	t.Helper()
	w := request(t, s, "GET", "/v1/files/"+id, p.Token, nil, 200)
	var f model.FileInfo
	if err := json.Unmarshal(w.Body.Bytes(), &f); err != nil {
		t.Fatal(err)
	}
	return f
}

func TestRegistrationRequiresSecretAndReplacesSession(t *testing.T) {
	s := newServer(t, Config{})
	a := registerPeer(t, s, model.Identity{})
	b := registerPeer(t, s, model.Identity{})
	if len(a.UUID) != 36 || a.UUID == b.UUID || a.Token == b.Token || a.DeviceSecret == "" || a.Token == a.DeviceSecret {
		t.Fatal("identities must be separate random values")
	}
	request(t, s, "POST", "/v1/register", "", model.RegisterRequest{Identity: model.Identity{UUID: a.UUID}}, 401)
	request(t, s, "POST", "/v1/register", "", model.RegisterRequest{Identity: model.Identity{UUID: a.UUID, DeviceSecret: b.DeviceSecret}}, 401)
	request(t, s, "POST", "/v1/heartbeat", a.UUID, nil, 401)
	next := registerPeer(t, s, a.Identity)
	if next.UUID != a.UUID || next.Token == a.Token {
		t.Fatal("reconnect did not rotate session")
	}
	request(t, s, "POST", "/v1/heartbeat", a.Token, nil, 401)
	request(t, s, "POST", "/v1/heartbeat", next.Token, nil, 200)
	var stored []byte
	if err := s.db.QueryRow("SELECT secret_hash FROM devices WHERE uuid=?", a.UUID).Scan(&stored); err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(stored, []byte(a.DeviceSecret)) || len(stored) != 32 {
		t.Fatal("plaintext secret stored")
	}
}
func TestManifestHoldingsBindingAndRevocation(t *testing.T) {
	s := newServer(t, Config{})
	a := registerPeer(t, s, model.Identity{})
	b := registerPeer(t, s, model.Identity{})
	m := manifest()
	request(t, s, "POST", "/v1/manifests", a.Token, m, 201)
	request(t, s, "POST", "/v1/manifests", a.Token, m, 204)
	invalid := m
	invalid.Name = "other.bin"
	request(t, s, "POST", "/v1/manifests", a.Token, invalid, 400)
	request(t, s, "PUT", "/v1/holdings", a.Token, model.HoldingsRequest{FileID: m.ID, Indices: []int{0, 1, 2}}, 204)
	request(t, s, "PUT", "/v1/holdings", b.Token, model.HoldingsRequest{FileID: m.ID, Indices: []int{2, 3}}, 204)
	f := fileInfo(t, s, a, m.ID)
	if len(f.Sources) != 2 {
		t.Fatalf("want 2 sources: %+v", f.Sources)
	}
	for _, indices := range [][]int{{0, 0}, {6}, {-1}} {
		request(t, s, "PUT", "/v1/holdings", a.Token, model.HoldingsRequest{FileID: m.ID, Indices: indices}, 400)
	}
	request(t, s, "PUT", "/v1/holdings", a.Token, map[string]any{"file_id": m.ID, "indices": []int{5}, "peer_id": b.UUID}, 400)
	request(t, s, "PUT", "/v1/holdings", a.Token, model.HoldingsRequest{FileID: m.ID, Indices: nil}, 204)
	f = fileInfo(t, s, b, m.ID)
	if len(f.Sources) != 1 || f.Sources[0].PeerID != b.UUID {
		t.Fatalf("revocation changed another peer: %+v", f.Sources)
	}
}
func TestExpiryAndHeartbeat(t *testing.T) {
	var clock atomic.Int64
	clock.Store(time.Now().Unix())
	now := func() time.Time { return time.Unix(clock.Load(), 0) }
	s := newServer(t, Config{Now: now, SessionTTL: time.Minute, OnlineTTL: 15 * time.Second})
	a := registerPeer(t, s, model.Identity{})
	b := registerPeer(t, s, model.Identity{})
	m := manifest()
	request(t, s, "POST", "/v1/manifests", a.Token, m, 201)
	request(t, s, "PUT", "/v1/holdings", a.Token, model.HoldingsRequest{FileID: m.ID, Indices: []int{0}}, 204)
	clock.Add(16)
	if len(fileInfo(t, s, b, m.ID).Sources) != 0 {
		t.Fatal("offline holdings still advertised")
	}
	request(t, s, "POST", "/v1/heartbeat", a.Token, nil, 200)
	if len(fileInfo(t, s, b, m.ID).Sources) != 1 {
		t.Fatal("heartbeat did not restore online state")
	}
	clock.Add(45)
	request(t, s, "POST", "/v1/heartbeat", a.Token, nil, 401)
}
func TestRestartPersistsMetadataButRequiresHoldingsReconfirmation(t *testing.T) {
	path := filepath.Join(t.TempDir(), "metadata.db")
	s := newServer(t, Config{DatabasePath: path})
	a := registerPeer(t, s, model.Identity{})
	m := manifest()
	request(t, s, "POST", "/v1/manifests", a.Token, m, 201)
	request(t, s, "PUT", "/v1/holdings", a.Token, model.HoldingsRequest{FileID: m.ID, Indices: []int{0, 1}}, 204)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	next := newServer(t, Config{DatabasePath: path})
	request(t, next, "GET", "/v1/files", a.Token, nil, 401)
	again := registerPeer(t, next, a.Identity)
	if len(fileInfo(t, next, again, m.ID).Sources) != 0 {
		t.Fatal("stale holdings trusted after restart/reconnect")
	}
	request(t, next, "PUT", "/v1/holdings", again.Token, model.HoldingsRequest{FileID: m.ID, Indices: []int{0, 1}}, 204)
	if len(fileInfo(t, next, again, m.ID).Sources) != 1 {
		t.Fatal("reconfirmation failed")
	}
}
func TestSignalsBoundToAuthenticatedSenderAndQueue(t *testing.T) {
	s := newServer(t, Config{MaxSignals: 1})
	a := registerPeer(t, s, model.Identity{})
	b := registerPeer(t, s, model.Identity{})
	sig := model.Signal{ID: "request1", From: b.UUID, To: b.UUID, Type: "offer", SDP: "v=0\r\n"}
	request(t, s, "POST", "/v1/signals", a.Token, sig, 202)
	request(t, s, "POST", "/v1/signals", a.Token, sig, 429)
	w := request(t, s, "GET", "/v1/signals", b.Token, nil, 200)
	var got []model.Signal
	if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].From != a.UUID {
		t.Fatalf("sender was spoofed: %+v", got)
	}
	w = request(t, s, "GET", "/v1/signals", b.Token, nil, 200)
	if strings.TrimSpace(w.Body.String()) != "[]" {
		t.Fatal("inbox did not drain")
	}
}
func TestLimitsAndMalformedJSON(t *testing.T) {
	s := newServer(t, Config{MaxSessions: 1, MaxBodyBytes: 256})
	a := registerPeer(t, s, model.Identity{})
	request(t, s, "POST", "/v1/register", "", model.RegisterRequest{}, 503)
	request(t, s, "POST", "/v1/manifests", a.Token, map[string]string{"huge": strings.Repeat("x", 512)}, 413)
	for _, body := range []string{"{} {}", "{broken", "null"} {
		r := httptest.NewRequest("POST", "/v1/manifests", strings.NewReader(body))
		r.Header.Set("Authorization", "Bearer "+a.Token)
		w := httptest.NewRecorder()
		s.Handler().ServeHTTP(w, r)
		if w.Code != http.StatusBadRequest {
			t.Errorf("%q: %d", body, w.Code)
		}
	}
	r := httptest.NewRequest("POST", "/v1/register", strings.NewReader(`{"name":"`+strings.Repeat("x", 512)+`"}`))
	w := httptest.NewRecorder()
	s.Handler().ServeHTTP(w, r)
	if w.Code != 413 {
		t.Fatalf("oversize body: %d %s", w.Code, w.Body.String())
	}
}

func TestReconnectionAndSignalTTLDiscardStaleOffers(t *testing.T) {
	var clock atomic.Int64
	clock.Store(time.Now().Unix())
	s := newServer(t, Config{Now: func() time.Time { return time.Unix(clock.Load(), 0) }, SignalTTL: 2 * time.Second})
	a := registerPeer(t, s, model.Identity{})
	b := registerPeer(t, s, model.Identity{})
	sig := model.Signal{ID: "test", To: b.UUID, Type: "offer", SDP: "v=0\r\n"}
	request(t, s, "POST", "/v1/signals", a.Token, sig, 202)
	a = registerPeer(t, s, a.Identity)
	w := request(t, s, "GET", "/v1/signals", b.Token, nil, 200)
	if strings.TrimSpace(w.Body.String()) != "[]" {
		t.Fatal("offer from replaced session delivered")
	}
	request(t, s, "POST", "/v1/signals", a.Token, sig, 202)
	clock.Add(3)
	w = request(t, s, "GET", "/v1/signals", b.Token, nil, 200)
	if strings.TrimSpace(w.Body.String()) != "[]" {
		t.Fatal("expired offer delivered")
	}
}
func TestHealthAndAuthenticationBoundary(t *testing.T) {
	s := newServer(t, Config{})
	request(t, s, "GET", "/healthz", "", nil, 200)
	for _, endpoint := range []struct{ method, path string }{{"GET", "/v1/files"}, {"POST", "/v1/manifests"}, {"PUT", "/v1/holdings"}, {"POST", "/v1/heartbeat"}, {"GET", "/v1/signals"}, {"POST", "/v1/signals"}} {
		request(t, s, endpoint.method, endpoint.path, "", nil, 401)
	}
}
