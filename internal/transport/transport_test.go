package transport

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Paulkm2006/p2pfs/internal/model"
	"github.com/Paulkm2006/p2pfs/internal/transport/testnet"
	stdnetv4 "github.com/pion/transport/v4/stdnet"
	"github.com/pion/turn/v4"
)

const testFileID = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

type signalBus struct {
	mu         sync.Mutex
	peers      map[string]*Transport
	dropDirect bool
	seen       []model.Signal
}
type endpoint struct {
	bus  *signalBus
	from string
}

func (e endpoint) Send(ctx context.Context, s model.Signal) error {
	e.bus.mu.Lock()
	// Simulate the coordinator binding the sender to its authenticated identity.
	s.From = e.from
	e.bus.seen = append(e.bus.seen, s)
	target := e.bus.peers[s.To]
	drop := e.bus.dropDirect && !s.Relay
	e.bus.mu.Unlock()
	if drop {
		return nil
	}
	if target == nil {
		return errors.New("offline peer")
	}
	return target.HandleSignal(ctx, s)
}
func newPair(t *testing.T, payload []byte, ice []model.ICEServer, clientOpts, serverOpts []Option) (*Transport, *Transport, *signalBus) {
	t.Helper()
	bus := &signalBus{peers: make(map[string]*Transport)}
	serverOpts = append(serverOpts, WithNetwork(testnet.Loopback()))
	clientOpts = append(clientOpts, WithNetwork(testnet.Loopback()))
	server, err := New("source", ice, endpoint{bus, "source"}, func(id string, index int) (io.ReadCloser, int64, error) {
		if id != testFileID || index != 2 {
			return nil, 0, errors.New("not held")
		}
		return io.NopCloser(bytes.NewReader(payload)), int64(len(payload)), nil
	}, serverOpts...)
	if err != nil {
		t.Fatal(err)
	}
	client, err := New("downloader", ice, endpoint{bus, "downloader"}, nil, clientOpts...)
	if err != nil {
		t.Fatal(err)
	}
	bus.peers["source"] = server
	bus.peers["downloader"] = client
	t.Cleanup(func() { _ = client.Close(); _ = server.Close() })
	return client, server, bus
}
func TestDirectStreaming(t *testing.T) {
	payload := make([]byte, 2*1024*1024+73)
	if _, err := rand.Read(payload); err != nil {
		t.Fatal(err)
	}
	client, _, bus := newPair(t, payload, nil, nil, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var out bytes.Buffer
	stats, err := client.Fetch(ctx, "source", testFileID, 2, &out)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(out.Bytes(), payload) || stats.Bytes != int64(len(payload)) || stats.Path != "direct" || stats.PeerID != "source" || stats.Duration <= 0 {
		t.Fatalf("bad transfer: %+v bytes=%d", stats, out.Len())
	}
	bus.mu.Lock()
	defer bus.mu.Unlock()
	for _, s := range bus.seen {
		if strings.Contains(strings.ToLower(s.SDP), " tcp ") || s.Relay {
			t.Fatal("direct connection advertised TCP or TURN")
		}
	}
}
func TestEmptyAndMissingShard(t *testing.T) {
	client, _, _ := newPair(t, nil, nil, nil, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	stats, err := client.Fetch(ctx, "source", testFileID, 2, io.Discard)
	if err != nil || stats.Bytes != 0 {
		t.Fatalf("empty: %+v %v", stats, err)
	}
	if _, err = client.Fetch(ctx, "source", testFileID, 1, io.Discard); err == nil || !strings.Contains(err.Error(), "unavailable") {
		t.Fatalf("missing shard: %v", err)
	}
}

type cancelWriter struct {
	cancel context.CancelFunc
	n      int
}

func (w *cancelWriter) Write(b []byte) (int, error) {
	w.n += len(b)
	if w.n >= 64*1024 {
		w.cancel()
	}
	return len(b), nil
}
func TestCancelStopsTransferAndReleasesSessions(t *testing.T) {
	payload := make([]byte, 8*1024*1024)
	client, server, _ := newPair(t, payload, nil, nil, []Option{WithUploadBytesPerSecond(1024 * 1024)})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	writer := &cancelWriter{cancel: cancel}
	started := time.Now()
	stats, err := client.Fetch(ctx, "source", testFileID, 2, writer)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("cancel: %+v %v", stats, err)
	}
	if stats.Bytes >= int64(len(payload)) || time.Since(started) > 3*time.Second {
		t.Fatal("cancellation did not stop promptly")
	}
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		client.mu.Lock()
		a := len(client.sessions)
		client.mu.Unlock()
		server.mu.Lock()
		b := len(server.sessions)
		server.mu.Unlock()
		if a == 0 && b == 0 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("sessions remain after cancellation")
}
func TestAggregateUploadPacing(t *testing.T) {
	payload := make([]byte, 32*1024)
	client, _, _ := newPair(t, payload, nil, nil, []Option{WithUploadBytesPerSecond(64 * 1024)})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	start := time.Now()
	if _, err := client.Fetch(ctx, "source", testFileID, 2, io.Discard); err != nil {
		t.Fatal(err)
	}
	if time.Since(start) < 450*time.Millisecond {
		t.Fatal("upload rate limit was not applied")
	}
}
func TestDirectConnectionTimeout(t *testing.T) {
	client, _, bus := newPair(t, nil, nil, []Option{WithDirectTimeout(80 * time.Millisecond)}, nil)
	bus.dropDirect = true
	start := time.Now()
	_, err := client.Fetch(context.Background(), "source", testFileID, 2, io.Discard)
	if !errors.Is(err, context.DeadlineExceeded) || time.Since(start) > time.Second {
		t.Fatalf("timeout: %v %s", err, time.Since(start))
	}
}
func localTURN(t *testing.T) []model.ICEServer {
	t.Helper()
	conn, err := net.ListenPacket("udp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	const user = "fixture"
	const password = "fixture-secret"
	const realm = "p2pfs-test"
	server, err := turn.NewServer(turn.ServerConfig{
		Realm: realm,
		AuthHandler: func(username, realm string, _ net.Addr) ([]byte, bool) {
			return turn.GenerateAuthKey(user, realm, password), username == user
		},
		PacketConnConfigs: []turn.PacketConnConfig{{PacketConn: conn, RelayAddressGenerator: &turn.RelayAddressGeneratorStatic{RelayAddress: net.ParseIP("127.0.0.1"), Address: "127.0.0.1", Net: &stdnetv4.Net{}}}},
	})
	if err != nil {
		_ = conn.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = server.Close(); _ = conn.Close() })
	return []model.ICEServer{{URLs: []string{"turn:" + conn.LocalAddr().String() + "?transport=udp"}, Username: user, Credential: password}}
}
func TestForcedRelayStreaming(t *testing.T) {
	ice := localTURN(t)
	payload := bytes.Repeat([]byte("encrypted relayed shard\x00"), 15000)
	client, _, bus := newPair(t, payload, ice, []Option{WithForceRelay(true)}, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	var out bytes.Buffer
	stats, err := client.Fetch(ctx, "source", testFileID, 2, &out)
	if err != nil {
		t.Fatal(err)
	}
	if stats.Path != "relay" || !bytes.Equal(out.Bytes(), payload) {
		t.Fatalf("bad relay transfer: %+v", stats)
	}
	bus.mu.Lock()
	defer bus.mu.Unlock()
	for _, s := range bus.seen {
		if !s.Relay {
			t.Fatal("both peers must enforce relay-only")
		}
	}
}
func TestAutomaticRelayFallback(t *testing.T) {
	ice := localTURN(t)
	payload := bytes.Repeat([]byte("fallback"), 9000)
	client, _, bus := newPair(t, payload, ice, []Option{WithDirectTimeout(100 * time.Millisecond)}, nil)
	bus.dropDirect = true
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	var out bytes.Buffer
	stats, err := client.Fetch(ctx, "source", testFileID, 2, &out)
	if err != nil || stats.Path != "relay" || !bytes.Equal(out.Bytes(), payload) {
		t.Fatalf("fallback: %+v %v", stats, err)
	}
	bus.mu.Lock()
	defer bus.mu.Unlock()
	if len(bus.seen) < 3 || bus.seen[0].Relay || !bus.seen[1].Relay {
		t.Fatalf("unexpected signaling: %+v", bus.seen)
	}
}
func TestValidationAndSenderBinding(t *testing.T) {
	bus := &signalBus{peers: make(map[string]*Transport)}
	for _, u := range []string{"turn:localhost?transport=tcp", "turns:localhost", "https://localhost", "stun:localhost?transport=tcp", "turn:localhost?transport=udp&transport=tcp"} {
		if tr, err := New("peer", []model.ICEServer{{URLs: []string{u}, Username: "u", Credential: "p"}}, endpoint{bus, "peer"}, nil); err == nil {
			_ = tr.Close()
			t.Errorf("accepted non-UDP URL: %s", u)
		}
	}
	client, _, _ := newPair(t, nil, nil, []Option{WithMaxConcurrentSessions(1)}, nil)
	s, err := client.newSession("transaction", "source", false, true)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = client.newSession("other", "source", false, true); !errors.Is(err, ErrBusy) {
		t.Fatalf("quota: %v", err)
	}
	for _, signal := range []model.Signal{
		{ID: s.id, From: "impostor", To: "downloader", Type: "answer", SDP: "invalid"},
		{ID: s.id, From: "source", To: "downloader", Type: "answer", SDP: "invalid", Relay: true},
		{ID: s.id, From: "source", To: "someone-else", Type: "answer", SDP: "invalid"},
	} {
		if err = client.HandleSignal(context.Background(), signal); err == nil {
			t.Fatal("accepted forged signal")
		}
	}
	s.cancel(context.Canceled)
	for _, index := range []int{-1, model.TotalShards} {
		if _, err = client.Fetch(context.Background(), "source", testFileID, index, io.Discard); err == nil {
			t.Fatal("accepted invalid index")
		}
	}
	for _, id := range []string{"../../secret", strings.Repeat("g", 64), strings.ToUpper(testFileID)} {
		if _, err = client.Fetch(context.Background(), "source", id, 2, io.Discard); err == nil {
			t.Fatal("accepted invalid file ID")
		}
	}
	if err = client.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err = client.Fetch(context.Background(), "source", testFileID, 2, io.Discard); !errors.Is(err, ErrClosed) {
		t.Fatalf("closed: %v", err)
	}
}
func TestControlDecoderRejectsTrailingUnknownAndOversize(t *testing.T) {
	for _, data := range []string{`{"file_id":"x","index":0,"unexpected":1}`, `{} {}`, strings.Repeat("x", maxControlSize+1)} {
		if decodeControl([]byte(data), new(request)) == nil {
			t.Fatal(fmt.Sprintf("accepted malformed control: %.80s", data))
		}
	}
}

func TestSourceLengthMismatch(t *testing.T) {
	for _, difference := range []int64{-1, 1} {
		t.Run(fmt.Sprint(difference), func(t *testing.T) {
			payload := bytes.Repeat([]byte("length"), 100)
			client, server, _ := newPair(t, payload, nil, nil, nil)
			server.open = func(string, int) (io.ReadCloser, int64, error) {
				return io.NopCloser(bytes.NewReader(payload)), int64(len(payload)) + difference, nil
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if _, err := client.Fetch(ctx, "source", testFileID, 2, io.Discard); err == nil {
				t.Fatal("accepted source with inconsistent advertised length")
			}
		})
	}
}

type trackedReader struct {
	*bytes.Reader
	closed chan struct{}
	once   sync.Once
}

func (r *trackedReader) Close() error { r.once.Do(func() { close(r.closed) }); return nil }
func TestStopSharingDuringOpen(t *testing.T) {
	client, server, _ := newPair(t, nil, nil, nil, nil)
	entered := make(chan struct{})
	release := make(chan struct{})
	r := &trackedReader{Reader: bytes.NewReader([]byte("private shard")), closed: make(chan struct{})}
	server.open = func(string, int) (io.ReadCloser, int64, error) { close(entered); <-release; return r, 13, nil }
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	result := make(chan error, 1)
	var output bytes.Buffer
	go func() { _, err := client.Fetch(ctx, "source", testFileID, 2, &output); result <- err }()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal("source never opened")
	}
	server.StopSharing(testFileID)
	close(release)
	select {
	case err := <-result:
		if err == nil {
			t.Fatal("unshared source completed")
		}
	case <-ctx.Done():
		t.Fatal("unshare did not stop transfer")
	}
	if output.Len() != 0 {
		t.Fatal("unshared bytes escaped after blocked OpenShard returned")
	}
	select {
	case <-r.closed:
	case <-ctx.Done():
		t.Fatal("unshared reader was not closed")
	}
}
func TestOfferReplayRejected(t *testing.T) {
	client, _, _ := newPair(t, nil, nil, nil, nil)
	s, err := client.newSession("replay-id", "source", false, true)
	if err != nil {
		t.Fatal(err)
	}
	s.cancel(context.Canceled)
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		client.mu.Lock()
		n := len(client.sessions)
		client.mu.Unlock()
		if n == 0 {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if _, err = client.newSession("replay-id", "source", false, true); err == nil || !strings.Contains(err.Error(), "replayed") {
		t.Fatalf("replay: %v", err)
	}
}

func TestSTUNGathersReflexiveUDP(t *testing.T) {
	ice := localTURN(t)
	address := strings.TrimSuffix(strings.TrimPrefix(ice[0].URLs[0], "turn:"), "?transport=udp")
	ice = append(ice, model.ICEServer{URLs: []string{"stun:" + address}})
	client, _, bus := newPair(t, []byte("STUN direct"), ice, nil, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	stats, err := client.Fetch(ctx, "source", testFileID, 2, io.Discard)
	if err != nil || stats.Path != "direct" {
		t.Fatalf("STUN-assisted direct: %+v %v", stats, err)
	}
	bus.mu.Lock()
	defer bus.mu.Unlock()
	found := false
	for _, s := range bus.seen {
		if strings.Contains(s.SDP, "typ srflx") {
			found = true
		}
	}
	if !found {
		t.Fatal("no server-reflexive candidate gathered from STUN")
	}
}
