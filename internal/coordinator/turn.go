package coordinator

import (
	"errors"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/pion/logging"
	"github.com/pion/transport/v4"
	"github.com/pion/turn/v4"
)

const turnRealm = "p2pfs"

// TURNConfig enables the embedded UDP-only relay. Both ends must use relay ICE
// candidates: traffic may only target another live allocation on this server.
// AllowLoopback is exclusively for explicit local development/tests, never a
// switch to allow arbitrary private-network relay destinations.
type TURNConfig struct {
	// Network permits an injected socket provider in constrained test runners.
	// Production callers normally leave this nil for Pion's standard network.
	Network                    transport.Net
	ListenAddress              string
	PublicIP                   string
	AdvertiseAddress           string
	AllowLoopback              bool
	RelayPortMin, RelayPortMax uint16
	MaxAllocations             int
	MaxAllocationsPerSession   int
	BytesPerSecond             int64
	SessionBytesPerSecond      int64
	IdleTimeout                time.Duration
}
type relayServer struct {
	owner        *Server
	cfg          TURNConfig
	server       *turn.Server
	listener     net.PacketConn
	generator    turn.RelayAddressGenerator
	ip           net.IP
	localIP      net.IP
	url          string
	mu           sync.Mutex
	allocations  map[string]*relayConn
	closed       bool
	second       int64
	bytes        int64
	sessionBytes map[string]int64
}
type relayConn struct {
	net.PacketConn
	server    *relayServer
	address   string
	username  string // protected by server.mu
	lastUsed  time.Time
	closeOnce sync.Once
}

func (s *Server) startTURN(cfg TURNConfig) error {
	if cfg.ListenAddress == "" {
		cfg.ListenAddress = "0.0.0.0:3478"
	}
	if cfg.MaxAllocations == 0 {
		cfg.MaxAllocations = 128
	}
	if cfg.MaxAllocationsPerSession == 0 {
		cfg.MaxAllocationsPerSession = 8
	}
	if cfg.BytesPerSecond == 0 {
		cfg.BytesPerSecond = 64 << 20
	}
	if cfg.SessionBytesPerSecond == 0 {
		cfg.SessionBytesPerSecond = 8 << 20
	}
	if cfg.IdleTimeout == 0 {
		cfg.IdleTimeout = 90 * time.Second
	}
	if cfg.MaxAllocations < 1 || cfg.MaxAllocationsPerSession < 1 || cfg.BytesPerSecond < 1 || cfg.SessionBytesPerSecond < 1 || cfg.IdleTimeout <= 0 {
		return errors.New("invalid TURN resource limits")
	}
	ip := net.ParseIP(cfg.PublicIP)
	if ip == nil || ip.To4() == nil {
		return errors.New("TURN PublicIP must be an explicit IPv4 address")
	}
	if !publicIP(ip) && !(cfg.AllowLoopback && ip.IsLoopback()) {
		return errors.New("TURN PublicIP must be public; loopback requires AllowLoopback")
	}
	host, _, err := net.SplitHostPort(cfg.ListenAddress)
	if err != nil {
		return err
	}
	if net.ParseIP(host) == nil {
		return errors.New("TURN listen address must use an IP literal")
	}
	if cfg.AllowLoopback && !net.ParseIP(host).IsLoopback() {
		return errors.New("TURN AllowLoopback requires a loopback listener")
	}
	if cfg.RelayPortMin > cfg.RelayPortMax || (cfg.RelayPortMin == 0) != (cfg.RelayPortMax == 0) || cfg.RelayPortMax == 65535 {
		return errors.New("invalid TURN relay port range (maximum 65534)")
	}
	conn, err := net.ListenPacket("udp4", cfg.ListenAddress)
	if err != nil {
		return err
	}
	advertised := cfg.AdvertiseAddress
	if advertised == "" {
		advertised = net.JoinHostPort(ip.String(), strconv.Itoa(conn.LocalAddr().(*net.UDPAddr).Port))
	}
	advHost, advPort, err := net.SplitHostPort(advertised)
	if err != nil || advHost == "" || strings.ContainsAny(advertised, "/?#\r\n") {
		conn.Close()
		return errors.New("TURN AdvertiseAddress must be host:port")
	}
	port, err := strconv.Atoi(advPort)
	if err != nil || port < 1 || port > 65535 {
		conn.Close()
		return errors.New("invalid TURN advertised port")
	}
	localIP := net.ParseIP(host)
	if localIP.IsUnspecified() {
		localIP = net.IPv4(127, 0, 0, 1)
	}
	r := &relayServer{owner: s, cfg: cfg, listener: conn, ip: ip, localIP: localIP, url: "turn:" + advertised + "?transport=udp", allocations: map[string]*relayConn{}, sessionBytes: map[string]int64{}}
	if cfg.RelayPortMin != 0 {
		r.generator = &turn.RelayAddressGeneratorPortRange{RelayAddress: ip, Address: host, MinPort: cfg.RelayPortMin, MaxPort: cfg.RelayPortMax, Net: cfg.Network}
	} else {
		r.generator = &turn.RelayAddressGeneratorStatic{RelayAddress: ip, Address: host, Net: cfg.Network}
	}
	ts, err := turn.NewServer(turn.ServerConfig{
		Realm: turnRealm, LoggerFactory: logging.NewDefaultLoggerFactory(), InboundMTU: 1500, ChannelBindTimeout: time.Minute,
		AuthHandler: func(username, realm string, src net.Addr) ([]byte, bool) {
			if realm != turnRealm {
				return nil, false
			}
			s.mu.Lock()
			defer s.mu.Unlock()
			p := s.turnUsers[username]
			if p == nil || !s.liveLocked(p) {
				return nil, false
			}
			return turn.GenerateAuthKey(username, realm, p.turnPassword), true
		},
		QuotaHandler: func(username, realm string, src net.Addr) bool {
			r.mu.Lock()
			defer r.mu.Unlock()
			if r.closed || len(r.allocations) >= cfg.MaxAllocations {
				return false
			}
			count := 0
			for _, a := range r.allocations {
				if a.username == username {
					count++
				}
			}
			return count < cfg.MaxAllocationsPerSession
		},
		EventHandler: turn.EventHandler{
			OnAllocationCreated: func(src, dst net.Addr, protocol, username, realm string, relay net.Addr, requestedPort int) {
				r.mu.Lock()
				if a := r.allocations[relay.String()]; a != nil {
					a.username = username
				}
				r.mu.Unlock()
			},
			OnChannelCreated: func(src, dst net.Addr, protocol, username, realm string, relay, peer net.Addr, channel uint16) {
				// Pion's permission callback sees only IPs. Rejecting an invalid
				// exact channel target also bounds channel state to live allocations.
				r.mu.Lock()
				a, target := r.allocations[relay.String()], r.allocations[peer.String()]
				valid := target != nil && target.username != "" && r.active(target.username)
				r.mu.Unlock()
				if !valid && a != nil {
					_ = a.Close()
				}
			},
		},
		PacketConnConfigs: []turn.PacketConnConfig{{PacketConn: conn, RelayAddressGenerator: r, PermissionHandler: func(client net.Addr, peer net.IP) bool { return peer.Equal(ip) }}},
	})
	if err != nil {
		conn.Close()
		return err
	}
	r.server = ts
	s.turn = r
	return nil
}

// publicIP excludes special-use ranges as well as private, loopback and link-local.
func publicIP(ip net.IP) bool {
	if !ip.IsGlobalUnicast() || ip.IsPrivate() || ip.IsLoopback() || ip.IsLinkLocalUnicast() {
		return false
	}
	v := ip.To4()
	if v == nil {
		return false
	}
	for _, cidr := range []string{"0.0.0.0/8", "100.64.0.0/10", "192.0.0.0/24", "192.0.2.0/24", "198.18.0.0/15", "198.51.100.0/24", "203.0.113.0/24", "240.0.0.0/4"} {
		_, n, _ := net.ParseCIDR(cidr)
		if n.Contains(ip) {
			return false
		}
	}
	return true
}
func (s *Server) liveLocked(p *session) bool {
	now := s.cfg.Now()
	return !s.closed && s.sessions[p.tokenHash] == p && now.Before(p.expires) && now.Sub(p.lastSeen) < s.cfg.OnlineTTL
}
func (r *relayServer) active(username string) bool {
	r.owner.mu.Lock()
	defer r.owner.mu.Unlock()
	p := r.owner.turnUsers[username]
	return p != nil && r.owner.liveLocked(p)
}
func (r *relayServer) Validate() error { return r.generator.Validate() }
func (r *relayServer) AllocateConn(string, int) (net.Conn, net.Addr, error) {
	return nil, nil, errors.New("TCP relay is disabled")
}
func (r *relayServer) AllocatePacketConn(network string, port int) (net.PacketConn, net.Addr, error) {
	if network != "udp" && network != "udp4" {
		return nil, nil, errors.New("only UDP4 relays are supported")
	}
	if port != 0 && (r.cfg.RelayPortMin == 0 || port < int(r.cfg.RelayPortMin) || port > int(r.cfg.RelayPortMax)) {
		return nil, nil, errors.New("requested relay port is not permitted")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed || len(r.allocations) >= r.cfg.MaxAllocations {
		return nil, nil, errors.New("TURN allocation capacity reached")
	}
	c, addr, err := r.generator.AllocatePacketConn("udp4", port)
	if err != nil {
		return nil, nil, err
	}
	a := &relayConn{PacketConn: c, server: r, address: addr.String(), lastUsed: time.Now()}
	r.allocations[a.address] = a
	return a, addr, nil
}

// allowed authenticates both allocations on every forwarded packet, including
// existing TURN channel bindings after a session was revoked or expired.
func (a *relayConn) allowed(addr net.Addr, n int, charge bool) bool {
	r := a.server
	r.mu.Lock()
	defer r.mu.Unlock()
	peer := r.allocations[addr.String()]
	if r.closed || peer == nil || a.username == "" || peer.username == "" || !r.active(a.username) || !r.active(peer.username) || n > 1500 {
		return false
	}
	if charge {
		now := time.Now().Unix()
		if r.second != now {
			r.second = now
			r.bytes = 0
			clear(r.sessionBytes)
		}
		size := int64(n)
		if r.bytes+size > r.cfg.BytesPerSecond || r.sessionBytes[a.username]+size > r.cfg.SessionBytesPerSecond || r.sessionBytes[peer.username]+size > r.cfg.SessionBytesPerSecond {
			return false
		}
		r.bytes += size
		r.sessionBytes[a.username] += size
		r.sessionBytes[peer.username] += size
	}
	a.lastUsed = time.Now()
	peer.lastUsed = a.lastUsed
	return true
}
func (a *relayConn) WriteTo(p []byte, addr net.Addr) (int, error) {
	if !a.allowed(addr, len(p), true) {
		return len(p), nil
	} // UDP-style drop; upper transport congestion control handles loss.
	// The other endpoint is another socket in this process. Route locally rather
	// than requiring public-IP NAT hairpin support on the deployment host.
	udp, ok := addr.(*net.UDPAddr)
	if !ok {
		return 0, errors.New("expected UDP relay address")
	}
	return a.PacketConn.WriteTo(p, &net.UDPAddr{IP: a.server.localIP, Port: udp.Port})
}
func (a *relayConn) ReadFrom(p []byte) (int, net.Addr, error) {
	for {
		n, addr, err := a.PacketConn.ReadFrom(p)
		if err != nil {
			return n, addr, err
		}
		udp, ok := addr.(*net.UDPAddr)
		if !ok || !udp.IP.Equal(a.server.localIP) {
			continue
		}
		advertised := &net.UDPAddr{IP: a.server.ip, Port: udp.Port}
		if a.allowed(advertised, n, false) {
			return n, advertised, nil
		}
	}
}
func (a *relayConn) Close() error {
	var err error
	a.closeOnce.Do(func() {
		a.server.mu.Lock()
		delete(a.server.allocations, a.address)
		a.server.mu.Unlock()
		err = a.PacketConn.Close()
	})
	return err
}
func (r *relayServer) reap() {
	r.mu.Lock()
	expired := []*relayConn{}
	for _, a := range r.allocations {
		if time.Since(a.lastUsed) >= r.cfg.IdleTimeout || (a.username != "" && !r.active(a.username)) {
			expired = append(expired, a)
		}
	}
	r.mu.Unlock()
	for _, a := range expired {
		_ = a.Close()
	}
}
func (r *relayServer) Close() error {
	r.mu.Lock()
	r.closed = true
	r.mu.Unlock()
	if r.server != nil {
		return r.server.Close()
	}
	return r.listener.Close()
}

// TURNAddress returns the listener address for tests/diagnostics, never credentials.
func (s *Server) TURNAddress() string {
	if s.turn == nil {
		return ""
	}
	return s.turn.listener.LocalAddr().String()
}

// TURNURL is suitable for diagnostics; usernames/passwords only leave via registration.
func (s *Server) TURNURL() string {
	if s.turn == nil {
		return ""
	}
	return s.turn.url
}

var _ turn.RelayAddressGenerator = (*relayServer)(nil)
