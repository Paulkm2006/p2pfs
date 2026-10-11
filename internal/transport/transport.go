// Package transport streams shards through reliable, ordered WebRTC DataChannels.
// SCTP provides reliability and congestion control, DTLS encrypts/authenticates the
// channel against the fingerprint delivered by the authenticated signaling API,
// and ICE uses UDP exclusively (including TURN). No file bytes use HTTP or TCP.
package transport

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Paulkm2006/p2pfs/internal/model"
	piontransport "github.com/pion/transport/v5"
	"github.com/pion/webrtc/v4"
)

const (
	blockSize      = 16 * 1024
	bufferLimit    = 256 * 1024
	maxControlSize = 1024
	maxSDPSize     = 48 * 1024
	protocol       = "p2pfs-shard-v1"
	maxShardSize   = (model.MaxFileSize + model.DataShards - 1) / model.DataShards
)

var (
	ErrClosed   = errors.New("transport closed")
	ErrBusy     = errors.New("transport session limit reached")
	ErrProtocol = errors.New("invalid shard transfer protocol")
	ErrNoRelay  = errors.New("no UDP TURN relay configured")
)

type Signaler interface {
	Send(context.Context, model.Signal) error
}

// OpenShard must authorize the requested file/index against local shared holdings.
// Close must interrupt a pending Read. The transport never opens arbitrary paths.
type OpenShard func(fileID string, index int) (io.ReadCloser, int64, error)
type Stats struct {
	Bytes    int64
	Duration time.Duration
	Path     string
	PeerID   string
}
type Option func(*options)
type options struct {
	network        piontransport.Net
	forceRelay     bool
	directTimeout  time.Duration
	sessionTimeout time.Duration
	maxSessions    int
	uploadRate     int64
}

// WithNetwork injects a Pion network implementation for isolated integration
// tests. Production callers should leave it unset for normal interface discovery.
func WithNetwork(network piontransport.Net) Option { return func(o *options) { o.network = network } }

func WithForceRelay(force bool) Option         { return func(o *options) { o.forceRelay = force } }
func WithDirectTimeout(d time.Duration) Option { return func(o *options) { o.directTimeout = d } }

// WithUploadBytesPerSecond limits aggregate payload upload across all sessions;
// zero means unlimited. The queue and SCTP window remain bounded in either mode.
func WithUploadBytesPerSecond(n int64) Option   { return func(o *options) { o.uploadRate = n } }
func WithMaxConcurrentSessions(n int) Option    { return func(o *options) { o.maxSessions = n } }
func WithSessionTimeout(d time.Duration) Option { return func(o *options) { o.sessionTimeout = d } }

type Transport struct {
	peerID              string
	signaler            Signaler
	open                OpenShard
	api                 *webrtc.API
	directICE, relayICE []webrtc.ICEServer
	opts                options
	ctx                 context.Context
	cancel              context.CancelFunc
	mu                  sync.Mutex
	sessions            map[string]*session
	recent              map[string]time.Time
	closed              bool
	wg                  sync.WaitGroup
	paceMu              sync.Mutex
	nextSend            time.Time
}
type session struct {
	id, remote      string
	relay, outgoing bool
	pc              *webrtc.PeerConnection
	ctx             context.Context
	cancel          context.CancelCauseFunc
	opened          chan struct{}
	messages        chan webrtc.DataChannelMessage
	low             chan struct{}
	dc              *webrtc.DataChannel
	channelSet      atomic.Bool
	requestedFile   atomic.Value
	answerMu        sync.Mutex
	answered        bool
}

func New(peerID string, ice []model.ICEServer, signaler Signaler, open OpenShard, opts ...Option) (*Transport, error) {
	if !validPeer(peerID) || signaler == nil {
		return nil, errors.New("peer ID and signaler are required")
	}
	o := options{directTimeout: 8 * time.Second, sessionTimeout: 10 * time.Minute, maxSessions: 32}
	for _, opt := range opts {
		if opt != nil {
			opt(&o)
		}
	}
	if o.directTimeout <= 0 || o.sessionTimeout <= 0 || o.maxSessions < 1 || o.maxSessions > 1024 || o.uploadRate < 0 {
		return nil, errors.New("invalid transport options")
	}
	t := &Transport{peerID: peerID, signaler: signaler, open: open, opts: o, sessions: make(map[string]*session), recent: make(map[string]time.Time)}
	for _, server := range ice {
		for _, raw := range server.URLs {
			u, err := url.Parse(raw)
			if err != nil || (u.Scheme != "stun" && u.Scheme != "turn") || u.Opaque == "" || u.Fragment != "" {
				return nil, fmt.Errorf("only stun: and turn: UDP ICE URLs are supported: %q", raw)
			}
			q, err := url.ParseQuery(u.RawQuery)
			if err != nil || len(q) > 1 || (len(q) == 1 && (len(q["transport"]) != 1 || q.Get("transport") != "udp")) {
				return nil, fmt.Errorf("ICE transport must be UDP: %q", raw)
			}
			s := webrtc.ICEServer{URLs: []string{raw}, Username: server.Username, Credential: server.Credential}
			if u.Scheme == "turn" {
				if server.Username == "" || server.Credential == "" {
					return nil, errors.New("TURN credentials are required")
				}
				t.relayICE = append(t.relayICE, s)
			} else {
				t.directICE = append(t.directICE, s)
			}
		}
	}
	if o.forceRelay && len(t.relayICE) == 0 {
		return nil, ErrNoRelay
	}
	settings := webrtc.SettingEngine{}
	if o.network != nil {
		settings.SetNet(o.network)
	}
	settings.SetNetworkTypes([]webrtc.NetworkType{webrtc.NetworkTypeUDP4, webrtc.NetworkTypeUDP6})
	settings.SetIncludeLoopbackCandidate(true)
	settings.SetSCTPMaxReceiveBufferSize(bufferLimit)
	settings.SetSCTPMaxMessageSize(blockSize)
	settings.SetICETimeouts(5*time.Second, 10*time.Second, time.Second)
	t.api = webrtc.NewAPI(webrtc.WithSettingEngine(settings))
	t.ctx, t.cancel = context.WithCancel(context.Background())
	return t, nil
}

func validPeer(p string) bool {
	return len(p) > 0 && len(p) <= 128 && !strings.ContainsAny(p, " \t\r\n")
}
func validRequest(id string, index int) bool {
	if len(id) != 64 || strings.ToLower(id) != id || index < 0 || index >= model.TotalShards {
		return false
	}
	_, err := hex.DecodeString(id)
	return err == nil
}
func newID() (string, error) {
	b := make([]byte, 16)
	_, err := rand.Read(b)
	return hex.EncodeToString(b), err
}

func (t *Transport) newSession(id, remote string, relay, outgoing bool) (*session, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		return nil, ErrClosed
	}
	if _, exists := t.sessions[id]; exists {
		return nil, errors.New("duplicate signal transaction")
	}
	now := time.Now()
	for prior, expiry := range t.recent {
		if !expiry.After(now) {
			delete(t.recent, prior)
		}
	}
	if _, replay := t.recent[id]; replay {
		return nil, errors.New("replayed signal transaction")
	}
	if len(t.sessions) >= t.opts.maxSessions {
		return nil, ErrBusy
	}
	count := 0
	for _, s := range t.sessions {
		if s.remote == remote {
			count++
		}
	}
	if count >= 8 {
		return nil, ErrBusy
	}
	config := webrtc.Configuration{ICEServers: t.directICE}
	if relay {
		if len(t.relayICE) == 0 {
			return nil, ErrNoRelay
		}
		config.ICEServers = t.relayICE
		config.ICETransportPolicy = webrtc.ICETransportPolicyRelay
	}
	pc, err := t.api.NewPeerConnection(config)
	if err != nil {
		return nil, err
	}
	lifetime, stopLifetime := context.WithTimeout(t.ctx, t.opts.sessionTimeout)
	ctx, cancel := context.WithCancelCause(lifetime)
	s := &session{id: id, remote: remote, relay: relay, outgoing: outgoing, pc: pc, ctx: ctx, cancel: cancel, opened: make(chan struct{}), messages: make(chan webrtc.DataChannelMessage, bufferLimit/blockSize), low: make(chan struct{}, 1)}
	pc.OnConnectionStateChange(func(state webrtc.PeerConnectionState) {
		if state == webrtc.PeerConnectionStateFailed || state == webrtc.PeerConnectionStateClosed {
			s.cancel(errors.New("peer connection " + state.String()))
		}
	})
	pc.OnDataChannel(func(dc *webrtc.DataChannel) {
		if outgoing || !s.installChannel(dc) {
			s.cancel(ErrProtocol)
			return
		}
		go t.serve(s)
	})
	if len(t.recent) >= 4096 {
		var oldestID string
		var oldest time.Time
		for prior, expiry := range t.recent {
			if oldestID == "" || expiry.Before(oldest) {
				oldestID, oldest = prior, expiry
			}
		}
		delete(t.recent, oldestID)
	}
	t.recent[id] = now.Add(10 * time.Minute)
	t.sessions[id] = s
	t.wg.Add(1)
	go func() {
		defer t.wg.Done()
		<-ctx.Done()
		stopLifetime()
		_ = pc.Close()
		t.mu.Lock()
		delete(t.sessions, id)
		t.mu.Unlock()
	}()
	return s, nil
}

func (s *session) installChannel(dc *webrtc.DataChannel) bool {
	if dc.Label() != protocol || dc.Protocol() != protocol || !dc.Ordered() || dc.MaxPacketLifeTime() != nil || dc.MaxRetransmits() != nil || !s.channelSet.CompareAndSwap(false, true) {
		return false
	}
	s.dc = dc
	dc.SetBufferedAmountLowThreshold(bufferLimit / 2)
	dc.OnBufferedAmountLow(func() {
		select {
		case s.low <- struct{}{}:
		default:
		}
	})
	dc.OnOpen(func() { close(s.opened) })
	dc.OnClose(func() { s.cancel(io.ErrUnexpectedEOF) })
	dc.OnError(func(err error) { s.cancel(err) })
	dc.OnMessage(func(m webrtc.DataChannelMessage) {
		if len(m.Data) == 0 || (m.IsString && len(m.Data) > maxControlSize) || (!m.IsString && len(m.Data) > blockSize) {
			s.cancel(ErrProtocol)
			return
		}
		// Blocking here propagates receiver backpressure into the bounded SCTP window.
		// Cancellation always unblocks the callback before PeerConnection.Close waits.
		select {
		case s.messages <- m:
		case <-s.ctx.Done():
		}
	})
	return true
}

// HandleSignal accepts only messages already authenticated by the coordinator.
// The transaction and expected sender bind answers to the requested peer. SDP
// carries the DTLS fingerprint and must therefore arrive over authenticated TLS.
func (t *Transport) HandleSignal(ctx context.Context, signal model.Signal) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if !validPeer(signal.ID) || !validPeer(signal.From) || signal.From == t.peerID || signal.To != t.peerID || len(signal.SDP) == 0 || len(signal.SDP) > maxSDPSize {
		return errors.New("invalid signal envelope")
	}
	switch signal.Type {
	case "offer":
		if t.open == nil {
			return errors.New("sharing is disabled")
		}
		s, err := t.newSession(signal.ID, signal.From, signal.Relay, false)
		if err != nil {
			return err
		}
		go func() {
			setup, cancel := context.WithTimeout(s.ctx, t.opts.directTimeout+10*time.Second)
			defer cancel()
			if err := s.pc.SetRemoteDescription(webrtc.SessionDescription{Type: webrtc.SDPTypeOffer, SDP: signal.SDP}); err != nil {
				s.cancel(err)
				return
			}
			answer, err := s.pc.CreateAnswer(nil)
			if err == nil {
				err = setLocal(setup, s, answer)
			}
			if err == nil {
				err = t.signaler.Send(setup, model.Signal{ID: s.id, From: t.peerID, To: s.remote, Type: "answer", SDP: s.pc.LocalDescription().SDP, Relay: s.relay})
			}
			if err != nil {
				s.cancel(err)
				return
			}
			select {
			case <-s.opened:
			case <-setup.Done():
				s.cancel(setup.Err())
			}
		}()
		return nil
	case "answer":
		t.mu.Lock()
		s := t.sessions[signal.ID]
		t.mu.Unlock()
		if s == nil || !s.outgoing || s.remote != signal.From || s.relay != signal.Relay {
			return errors.New("answer does not match requested peer and transaction")
		}
		s.answerMu.Lock()
		defer s.answerMu.Unlock()
		if s.answered {
			return errors.New("duplicate answer")
		}
		if err := s.pc.SetRemoteDescription(webrtc.SessionDescription{Type: webrtc.SDPTypeAnswer, SDP: signal.SDP}); err != nil {
			s.cancel(err)
			return err
		}
		s.answered = true
		return nil
	default:
		return errors.New("unsupported signal type")
	}
}

func setLocal(ctx context.Context, s *session, desc webrtc.SessionDescription) error {
	complete := webrtc.GatheringCompletePromise(s.pc)
	if err := s.pc.SetLocalDescription(desc); err != nil {
		return err
	}
	select {
	case <-complete:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-s.ctx.Done():
		return context.Cause(s.ctx)
	}
}

func (t *Transport) connect(ctx context.Context, peer string, relay bool) (*session, error) {
	id, err := newID()
	if err != nil {
		return nil, err
	}
	s, err := t.newSession(id, peer, relay, true)
	if err != nil {
		return nil, err
	}
	ok := false
	defer func() {
		if !ok {
			s.cancel(errors.New("connection attempt ended"))
		}
	}()
	ordered := true
	proto := protocol
	dc, err := s.pc.CreateDataChannel(protocol, &webrtc.DataChannelInit{Ordered: &ordered, Protocol: &proto})
	if err != nil {
		return nil, err
	}
	if !s.installChannel(dc) {
		return nil, ErrProtocol
	}
	offer, err := s.pc.CreateOffer(nil)
	if err == nil {
		err = setLocal(ctx, s, offer)
	}
	if err == nil {
		err = t.signaler.Send(ctx, model.Signal{ID: s.id, From: t.peerID, To: peer, Type: "offer", SDP: s.pc.LocalDescription().SDP, Relay: relay})
	}
	if err != nil {
		return nil, err
	}
	select {
	case <-s.opened:
		ok = true
		return s, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-s.ctx.Done():
		return nil, context.Cause(s.ctx)
	}
}

// Fetch opens a direct-only connection first. Only connection setup is retried
// through TURN, before any bytes reach dst; partial transfers are never appended
// to a relay retry. The caller must verify length/hash against the trusted manifest.
func (t *Transport) Fetch(ctx context.Context, peerID, fileID string, index int, dst io.Writer) (stats Stats, err error) {
	start := time.Now()
	stats.PeerID = peerID
	defer func() { stats.Duration = time.Since(start) }()
	if !validPeer(peerID) || peerID == t.peerID || !validRequest(fileID, index) || dst == nil {
		return stats, errors.New("invalid shard request")
	}
	if err = ctx.Err(); err != nil {
		return stats, err
	}
	var s *session
	if !t.opts.forceRelay {
		attempt, cancel := context.WithTimeout(ctx, t.opts.directTimeout)
		s, err = t.connect(attempt, peerID, false)
		cancel()
	}
	if s == nil && ctx.Err() == nil && len(t.relayICE) > 0 {
		attempt, cancel := context.WithTimeout(ctx, t.opts.directTimeout+10*time.Second)
		s, err = t.connect(attempt, peerID, true)
		cancel()
	}
	if s == nil {
		if ctx.Err() != nil {
			err = ctx.Err()
		}
		return stats, err
	}
	stop := context.AfterFunc(ctx, func() { s.cancel(ctx.Err()) })
	defer stop()
	defer s.cancel(errors.New("fetch finished"))
	pair, err := s.pc.SCTP().Transport().ICETransport().GetSelectedCandidatePair()
	if err != nil || pair == nil {
		return stats, errors.New("selected ICE path unavailable")
	}
	if pair.Local.Protocol != webrtc.ICEProtocolUDP || pair.Remote.Protocol != webrtc.ICEProtocolUDP {
		return stats, errors.New("non-UDP ICE path rejected")
	}
	stats.Path = "direct"
	if pair.Local.Typ == webrtc.ICECandidateTypeRelay || pair.Remote.Typ == webrtc.ICECandidateTypeRelay {
		stats.Path = "relay"
	}
	if err = s.sendControl(request{FileID: fileID, Index: index}); err != nil {
		return stats, err
	}
	m, err := s.receive()
	if err != nil {
		return stats, err
	}
	var head header
	if !m.IsString || decodeControl(m.Data, &head) != nil || head.Type != "header" {
		return stats, ErrProtocol
	}
	if head.Error != "" {
		return stats, fmt.Errorf("peer refused shard: %s", head.Error)
	}
	if head.Size < 0 || head.Size > maxShardSize {
		return stats, ErrProtocol
	}
	for {
		m, err = s.receive()
		if err != nil {
			return stats, err
		}
		if m.IsString {
			var end trailer
			if decodeControl(m.Data, &end) != nil || end.Type != "end" {
				return stats, ErrProtocol
			}
			if end.Error != "" {
				return stats, fmt.Errorf("peer shard read failed: %s", end.Error)
			}
			if end.Size != head.Size || stats.Bytes != head.Size {
				return stats, io.ErrUnexpectedEOF
			}
			return stats, nil
		}
		if stats.Bytes+int64(len(m.Data)) > head.Size {
			return stats, ErrProtocol
		}
		n, writeErr := dst.Write(m.Data)
		stats.Bytes += int64(n)
		if writeErr != nil {
			return stats, writeErr
		}
		if n != len(m.Data) {
			return stats, io.ErrShortWrite
		}
	}
}

type request struct {
	FileID string `json:"file_id"`
	Index  int    `json:"index"`
}
type header struct {
	Type  string `json:"type"`
	Size  int64  `json:"size"`
	Error string `json:"error,omitempty"`
}
type trailer struct {
	Type  string `json:"type"`
	Size  int64  `json:"size"`
	Error string `json:"error,omitempty"`
}

func decodeControl(data []byte, out any) error {
	if len(data) > maxControlSize {
		return ErrProtocol
	}
	d := json.NewDecoder(bytes.NewReader(data))
	d.DisallowUnknownFields()
	if err := d.Decode(out); err != nil {
		return err
	}
	if d.Decode(new(any)) != io.EOF {
		return ErrProtocol
	}
	return nil
}
func (s *session) receive() (webrtc.DataChannelMessage, error) {
	select {
	case m := <-s.messages:
		return m, nil
	case <-s.ctx.Done():
		return webrtc.DataChannelMessage{}, context.Cause(s.ctx)
	}
}
func (s *session) sendControl(v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	return s.send(b, true)
}
func (s *session) send(data []byte, text bool) error {
	for s.dc.BufferedAmount()+uint64(len(data)) > bufferLimit {
		select {
		case <-s.low:
		case <-s.ctx.Done():
			return context.Cause(s.ctx)
		}
	}
	if err := s.ctx.Err(); err != nil {
		return context.Cause(s.ctx)
	}
	if text {
		return s.dc.SendText(string(data))
	}
	return s.dc.Send(data)
}

func (t *Transport) serve(s *session) {
	select {
	case <-s.opened:
	case <-s.ctx.Done():
		return
	}
	// The source stays alive until the receiver closes after consuming the ordered
	// end frame. Closing immediately after Send would discard queued SCTP data.
	m, err := s.receive()
	if err != nil {
		return
	}
	var req request
	if !m.IsString || decodeControl(m.Data, &req) != nil || !validRequest(req.FileID, req.Index) {
		s.cancel(ErrProtocol)
		return
	}
	s.requestedFile.Store(req.FileID)
	if s.ctx.Err() != nil {
		return
	}
	r, size, err := t.open(req.FileID, req.Index)
	if s.ctx.Err() != nil {
		if r != nil {
			_ = r.Close()
		}
		return
	}
	if err != nil || r == nil || size < 0 || size > maxShardSize {
		if r != nil {
			_ = r.Close()
		}
		if sendErr := s.sendControl(header{Type: "header", Error: "shard unavailable"}); sendErr != nil {
			s.cancel(sendErr)
		}
		return
	}
	defer r.Close()
	stop := context.AfterFunc(s.ctx, func() { _ = r.Close() })
	defer stop()
	if err = s.sendControl(header{Type: "header", Size: size}); err != nil {
		s.cancel(err)
		return
	}
	block := blockSize
	if t.opts.uploadRate > 0 && t.opts.uploadRate/10+1 < int64(block) {
		block = int(t.opts.uploadRate/10 + 1)
	}
	buf := make([]byte, block)
	var sent int64
	for sent < size {
		want := int64(len(buf))
		if size-sent < want {
			want = size - sent
		}
		n, readErr := io.ReadFull(r, buf[:int(want)])
		if n > 0 {
			if err = t.pace(s.ctx, n); err == nil {
				err = s.send(buf[:n], false)
			}
			if err != nil {
				s.cancel(err)
				return
			}
			sent += int64(n)
		}
		if readErr != nil {
			_ = s.sendControl(trailer{Type: "end", Size: sent, Error: "short or unreadable shard"})
			return
		}
	}
	// Refuse sources whose advertised length omits trailing bytes.
	var extra [1]byte
	if n, e := r.Read(extra[:]); n != 0 || e != io.EOF {
		_ = s.sendControl(trailer{Type: "end", Size: sent, Error: "shard length changed"})
		return
	}
	if err = s.sendControl(trailer{Type: "end", Size: sent}); err != nil {
		s.cancel(err)
	}
}
func (t *Transport) pace(ctx context.Context, n int) error {
	if t.opts.uploadRate == 0 {
		return nil
	}
	t.paceMu.Lock()
	now := time.Now()
	if t.nextSend.Before(now) {
		t.nextSend = now
	}
	t.nextSend = t.nextSend.Add(time.Duration(float64(n) * float64(time.Second) / float64(t.opts.uploadRate)))
	delay := time.Until(t.nextSend)
	t.paceMu.Unlock()
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return context.Cause(ctx)
	}
}

// StopSharing cancels active incoming transfers for a file. The owner must also
// revoke the local holding before calling it, so new OpenShard calls reject it.
// An OpenShard callback already running cannot be interrupted by this API; its
// result is immediately closed without sending data if this session was canceled.
func (t *Transport) StopSharing(fileID string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, s := range t.sessions {
		if !s.outgoing && s.requestedFile.Load() == fileID {
			s.cancel(errors.New("file is no longer shared"))
		}
	}
}

func (t *Transport) Close() error {
	t.mu.Lock()
	if !t.closed {
		t.closed = true
		t.cancel()
	}
	t.mu.Unlock()
	t.wg.Wait()
	return nil
}
