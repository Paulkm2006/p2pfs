package coordinator

import (
	"bytes"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Paulkm2006/p2pfs/internal/model"
	"github.com/pion/logging"
	"github.com/pion/transport/v4/stdnet"
	"github.com/pion/turn/v4"
)

func allocate(t *testing.T, s *Server, p model.RegisterResponse) net.PacketConn {
	t.Helper()
	socket, err := net.ListenPacket("udp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { socket.Close() })
	ice := p.ICEServers[len(p.ICEServers)-1]
	c, err := turn.NewClient(&turn.ClientConfig{STUNServerAddr: s.TURNAddress(), TURNServerAddr: s.TURNAddress(), Username: ice.Username, Password: ice.Credential, Realm: turnRealm, Conn: socket, Net: &stdnet.Net{}, LoggerFactory: logging.NewDefaultLoggerFactory(), RTO: 50 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)
	if err = c.Listen(); err != nil {
		t.Fatal(err)
	}
	conn, err := c.Allocate()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	return conn
}
func TestTURNAuthenticatedUDPRelayAndTargetRestriction(t *testing.T) {
	s := newServer(t, Config{TURN: &TURNConfig{ListenAddress: "127.0.0.1:0", PublicIP: "127.0.0.1", AllowLoopback: true, Network: &stdnet.Net{}}})
	a := registerPeer(t, s, model.Identity{})
	b := registerPeer(t, s, model.Identity{})
	left := allocate(t, s, a)
	right := allocate(t, s, b)
	// Establish permissions in each direction before sending application data.
	payload := []byte("encrypted application data")
	right.WriteTo([]byte("probe"), left.LocalAddr())
	left.WriteTo(payload, right.LocalAddr())
	right.SetReadDeadline(time.Now().Add(3 * time.Second))
	buf := make([]byte, 1600)
	n, _, err := right.ReadFrom(buf)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(buf[:n], payload) {
		t.Fatalf("relay payload differs: %q", buf[:n])
	}
	unrelated, err := net.ListenPacket("udp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer unrelated.Close()
	unrelated.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
	left.WriteTo([]byte("must not reach arbitrary services"), unrelated.LocalAddr())
	if _, _, err = unrelated.ReadFrom(buf); err == nil {
		t.Fatal("TURN forwarded to an unregistered local service")
	}
}
func TestTURNRejectsExpiredCredentialsAndExistingData(t *testing.T) {
	var clock atomic.Int64
	clock.Store(time.Now().Unix())
	s := newServer(t, Config{Now: func() time.Time { return time.Unix(clock.Load(), 0) }, SessionTTL: time.Minute, OnlineTTL: time.Minute, TURN: &TURNConfig{ListenAddress: "127.0.0.1:0", PublicIP: "127.0.0.1", AllowLoopback: true, Network: &stdnet.Net{}}})
	a := registerPeer(t, s, model.Identity{})
	b := registerPeer(t, s, model.Identity{})
	left := allocate(t, s, a)
	right := allocate(t, s, b)
	right.WriteTo([]byte("probe"), left.LocalAddr())
	clock.Add(61)
	left.WriteTo([]byte("expired"), right.LocalAddr())
	right.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
	buf := make([]byte, 1600)
	if _, _, err := right.ReadFrom(buf); err == nil {
		t.Fatal("expired relay traffic passed")
	}
	s.mu.Lock()
	p := s.turnUsers[a.ICEServers[0].Username]
	live := p != nil && s.liveLocked(p)
	s.mu.Unlock()
	if live {
		t.Fatal("expired TURN credentials remained valid")
	}
}
func TestTURNConfigRejectsPrivateTargetsByDefault(t *testing.T) {
	for _, ip := range []string{"127.0.0.1", "10.0.0.1", "169.254.169.254", "100.64.0.1", "192.0.2.1", "224.0.0.1"} {
		s, err := New(Config{DatabasePath: ":memory:", TURN: &TURNConfig{ListenAddress: "127.0.0.1:0", PublicIP: ip}})
		if err == nil {
			s.Close()
			t.Fatalf("accepted unsafe public IP %s", ip)
		}
	}
}

func TestTURNAllocationAndByteBudgets(t *testing.T) {
	s := newServer(t, Config{TURN: &TURNConfig{ListenAddress: "127.0.0.1:0", PublicIP: "127.0.0.1", AllowLoopback: true, Network: &stdnet.Net{}, MaxAllocations: 2, BytesPerSecond: 100, SessionBytesPerSecond: 60}})
	a := registerPeer(t, s, model.Identity{})
	b := registerPeer(t, s, model.Identity{})
	left := allocate(t, s, a)
	right := allocate(t, s, b)
	if _, _, err := s.turn.AllocatePacketConn("udp4", 0); err == nil {
		t.Fatal("allocation cap not enforced")
	}
	s.turn.mu.Lock()
	allocation := s.turn.allocations[left.LocalAddr().String()]
	s.turn.mu.Unlock()
	if !allocation.allowed(right.LocalAddr(), 40, true) {
		t.Fatal("valid packet rejected")
	}
	if allocation.allowed(right.LocalAddr(), 40, true) {
		t.Fatal("session byte budget not enforced")
	}
	if allocation.allowed(right.LocalAddr(), 1501, false) {
		t.Fatal("oversized packet allowed")
	}
	if _, _, err := s.turn.AllocateConn("tcp", 0); err == nil {
		t.Fatal("TCP relay enabled")
	}
}
