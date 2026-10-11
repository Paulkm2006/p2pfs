// Package coordinator implements the metadata-only control plane and optional UDP TURN relay.
package coordinator

import (
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"database/sql"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/Paulkm2006/p2pfs/internal/codec"
	"github.com/Paulkm2006/p2pfs/internal/model"
	"github.com/labstack/echo/v5"
	_ "modernc.org/sqlite"
)

// Config defaults target one small trusted collaboration space. Registration has
// no accounts: restrict API access at the firewall or authenticated reverse proxy
// before exposing the service beyond trusted participants.
type Config struct {
	DatabasePath      string
	SessionTTL        time.Duration
	OnlineTTL         time.Duration
	HeartbeatInterval time.Duration
	SignalTTL         time.Duration
	STUNURLs          []string
	TURN              *TURNConfig
	MaxSessions       int
	MaxDevices        int
	MaxFiles          int
	MaxSignals        int
	MaxBodyBytes      int64
	// Now is injectable for deterministic expiry tests. Leave nil in production.
	Now func() time.Time
}

type session struct {
	uuid, tokenHash, turnUser, turnPassword string
	expires, lastSeen                       time.Time
	requests                                int
	requestSecond                           int64
	inbox                                   []queuedSignal
}
type queuedSignal struct {
	signal   model.Signal
	expires  time.Time
	fromHash string
}

type Server struct {
	cfg                        Config
	db                         *sql.DB
	echo                       *echo.Echo
	mu                         sync.Mutex
	sessions                   map[string]*session
	peers                      map[string]*session
	turnUsers                  map[string]*session
	closed                     bool
	closeOnce                  sync.Once
	closeErr                   error
	done                       chan struct{}
	wg                         sync.WaitGroup
	turn                       *relayServer
	rateSecond, registerMinute int64
	rateCount, registerCount   int
}

func New(cfg Config) (*Server, error) {
	if cfg.DatabasePath == "" {
		cfg.DatabasePath = "p2pfs.db"
	}
	if cfg.SessionTTL == 0 {
		cfg.SessionTTL = 24 * time.Hour
	}
	if cfg.OnlineTTL == 0 {
		cfg.OnlineTTL = 15 * time.Second
	}
	if cfg.HeartbeatInterval == 0 {
		cfg.HeartbeatInterval = 5 * time.Second
	}
	if cfg.SignalTTL == 0 {
		cfg.SignalTTL = time.Minute
	}
	if cfg.MaxSessions == 0 {
		cfg.MaxSessions = 64
	}
	if cfg.MaxDevices == 0 {
		cfg.MaxDevices = 1000
	}
	if cfg.MaxFiles == 0 {
		cfg.MaxFiles = 10000
	}
	if cfg.MaxSignals == 0 {
		cfg.MaxSignals = 64
	}
	if cfg.MaxBodyBytes == 0 {
		cfg.MaxBodyBytes = 64 << 10
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	if cfg.SessionTTL <= 0 || cfg.OnlineTTL <= 0 || cfg.HeartbeatInterval <= 0 || cfg.SignalTTL <= 0 || cfg.MaxSessions < 1 || cfg.MaxDevices < 1 || cfg.MaxFiles < 1 || cfg.MaxSignals < 1 || cfg.MaxBodyBytes < 1 {
		return nil, errors.New("invalid coordinator limits")
	}
	for _, u := range cfg.STUNURLs {
		if !strings.HasPrefix(u, "stun:") || strings.ContainsAny(u, "\r\n\x00") {
			return nil, errors.New("STUN URLs must use stun: (UDP)")
		}
	}
	if cfg.DatabasePath != ":memory:" {
		if err := os.MkdirAll(filepath.Dir(cfg.DatabasePath), 0700); err != nil {
			return nil, err
		}
		f, err := os.OpenFile(cfg.DatabasePath, os.O_CREATE|os.O_RDWR, 0600)
		if err != nil {
			return nil, err
		}
		f.Close()
		if err := os.Chmod(cfg.DatabasePath, 0600); err != nil {
			return nil, err
		}
	}
	db, err := sql.Open("sqlite", cfg.DatabasePath)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	_, err = db.Exec(`PRAGMA foreign_keys=ON; PRAGMA busy_timeout=5000; PRAGMA journal_mode=WAL;
 CREATE TABLE IF NOT EXISTS devices(uuid TEXT PRIMARY KEY, secret_hash BLOB NOT NULL, name TEXT NOT NULL);
 CREATE TABLE IF NOT EXISTS manifests(id TEXT PRIMARY KEY, body BLOB NOT NULL);
 CREATE TABLE IF NOT EXISTS holdings(file_id TEXT NOT NULL REFERENCES manifests(id), peer_id TEXT NOT NULL REFERENCES devices(uuid), shard_index INTEGER NOT NULL, session_hash TEXT NOT NULL, PRIMARY KEY(file_id,peer_id,shard_index));`)
	if err != nil {
		db.Close()
		return nil, err
	}
	s := &Server{cfg: cfg, db: db, echo: echo.New(), sessions: map[string]*session{}, peers: map[string]*session{}, turnUsers: map[string]*session{}, done: make(chan struct{})}
	s.echo.Use(s.limit)
	s.echo.GET("/healthz", func(c *echo.Context) error { return c.JSON(http.StatusOK, map[string]string{"status": "ok"}) })
	s.echo.POST("/v1/register", s.register)
	api := s.echo.Group("/v1", s.authenticate)
	api.POST("/heartbeat", s.heartbeat)
	api.POST("/manifests", s.publish)
	api.GET("/files", s.files)
	api.GET("/files/:id", s.file)
	api.PUT("/holdings", s.holdings)
	api.POST("/signals", s.sendSignal)
	api.GET("/signals", s.receiveSignals)
	if cfg.TURN != nil {
		if err := s.startTURN(*cfg.TURN); err != nil {
			db.Close()
			return nil, err
		}
	}
	s.wg.Add(1)
	go s.reap()
	return s, nil
}

func (s *Server) Handler() http.Handler { return s.echo }
func (s *Server) Close() error {
	s.closeOnce.Do(func() {
		close(s.done)
		s.wg.Wait()
		s.mu.Lock()
		s.closed = true
		s.sessions = map[string]*session{}
		s.peers = map[string]*session{}
		s.turnUsers = map[string]*session{}
		s.mu.Unlock()
		if s.turn != nil {
			s.closeErr = s.turn.Close()
		}
		s.closeErr = errors.Join(s.closeErr, s.db.Close())
	})
	return s.closeErr
}

func token() (string, error) {
	var b [32]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(b[:]), nil
}
func uuid() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	b[6] = (b[6] & 15) | 64
	b[8] = (b[8] & 63) | 128
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[:4], b[4:6], b[6:8], b[8:10], b[10:]), nil
}
func digest(v string) string         { h := sha256.Sum256([]byte(v)); return hex.EncodeToString(h[:]) }
func bad(code int, msg string) error { return echo.NewHTTPError(code, msg) }
func (s *Server) decode(c *echo.Context, out any) error {
	c.Request().Body = http.MaxBytesReader(c.Response(), c.Request().Body, s.cfg.MaxBodyBytes)
	dec := json.NewDecoder(c.Request().Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(out); err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			return bad(413, "request body too large")
		}
		return bad(400, "invalid JSON request")
	}
	if err := dec.Decode(new(any)); err != io.EOF {
		return bad(400, "expected one JSON value")
	}
	return nil
}
func (s *Server) limit(next echo.HandlerFunc) echo.HandlerFunc {
	return func(c *echo.Context) error {
		c.Response().Header().Set("Cache-Control", "no-store")
		c.Response().Header().Set("X-Content-Type-Options", "nosniff")
		s.mu.Lock()
		now := s.cfg.Now().Unix()
		if s.rateSecond != now {
			s.rateSecond = now
			s.rateCount = 0
		}
		s.rateCount++
		blocked := s.rateCount > 500 || s.closed
		s.mu.Unlock()
		if blocked {
			return bad(429, "server request limit reached")
		}
		return next(c)
	}
}
func (s *Server) authenticate(next echo.HandlerFunc) echo.HandlerFunc {
	return func(c *echo.Context) error {
		h := c.Request().Header.Get("Authorization")
		if !strings.HasPrefix(h, "Bearer ") || len(h) > 256 {
			return bad(401, "valid bearer session required")
		}
		hash := digest(strings.TrimPrefix(h, "Bearer "))
		now := s.cfg.Now()
		s.mu.Lock()
		p, ok := s.sessions[hash]
		if !ok || !now.Before(p.expires) || s.closed {
			s.mu.Unlock()
			return bad(401, "session expired or invalid")
		}
		if p.requestSecond != now.Unix() {
			p.requestSecond = now.Unix()
			p.requests = 0
		}
		p.requests++
		over := p.requests > 100
		s.mu.Unlock()
		if over {
			return bad(429, "session request limit reached")
		}
		c.Set("session", p)
		return next(c)
	}
}
func (s *Server) currentLocked(c *echo.Context) (*session, error) {
	p, ok := c.Get("session").(*session)
	if !ok || s.sessions[p.tokenHash] != p || !s.cfg.Now().Before(p.expires) || s.closed {
		return nil, bad(401, "session expired or replaced")
	}
	return p, nil
}
func (s *Server) cleanupLocked(now time.Time) {
	for hash, p := range s.sessions {
		if !now.Before(p.expires) {
			delete(s.sessions, hash)
			delete(s.peers, p.uuid)
			delete(s.turnUsers, p.turnUser)
		} else {
			p.inbox = prune(p.inbox, now)
		}
	}
}
func prune(q []queuedSignal, now time.Time) []queuedSignal {
	out := q[:0]
	for _, v := range q {
		if now.Before(v.expires) {
			out = append(out, v)
		}
	}
	clear(q[len(out):])
	return out
}
func (s *Server) reap() {
	defer s.wg.Done()
	t := time.NewTicker(time.Second)
	defer t.Stop()
	for {
		select {
		case <-s.done:
			return
		case <-t.C:
			s.mu.Lock()
			s.cleanupLocked(s.cfg.Now())
			s.mu.Unlock()
			if s.turn != nil {
				s.turn.reap()
			}
		}
	}
}

func (s *Server) register(c *echo.Context) error {
	var req model.RegisterRequest
	if err := s.decode(c, &req); err != nil {
		return err
	}
	if len(req.Name) > 128 || strings.ContainsAny(req.Name, "\x00\r\n") {
		return bad(400, "invalid device name")
	}
	if (req.UUID == "") != (req.DeviceSecret == "") || len(req.UUID) > 36 || len(req.DeviceSecret) > 128 {
		return bad(401, "UUID and device secret must be supplied together")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.cfg.Now()
	s.cleanupLocked(now)
	minute := now.Unix() / 60
	if minute != s.registerMinute {
		s.registerMinute = minute
		s.registerCount = 0
	}
	s.registerCount++
	if s.registerCount > 120 {
		return bad(429, "registration rate limit reached")
	}
	if len(s.sessions) >= s.cfg.MaxSessions && s.peers[req.UUID] == nil {
		return bad(503, "session capacity reached")
	}
	id, secret := req.UUID, req.DeviceSecret
	if id == "" {
		var count int
		if err := s.db.QueryRow("SELECT COUNT(*) FROM devices").Scan(&count); err != nil {
			return err
		}
		if count >= s.cfg.MaxDevices {
			return bad(503, "device capacity reached")
		}
		var err error
		id, err = uuid()
		if err != nil {
			return err
		}
		secret, err = token()
		if err != nil {
			return err
		}
		hash := sha256.Sum256([]byte(secret))
		if _, err = s.db.Exec("INSERT INTO devices(uuid,secret_hash,name) VALUES(?,?,?)", id, hash[:], req.Name); err != nil {
			return err
		}
	} else {
		var want []byte
		err := s.db.QueryRow("SELECT secret_hash FROM devices WHERE uuid=?", id).Scan(&want)
		got := sha256.Sum256([]byte(secret))
		if err != nil || subtle.ConstantTimeCompare(want, got[:]) != 1 {
			return bad(401, "invalid device identity")
		}
		if _, err = s.db.Exec("UPDATE devices SET name=? WHERE uuid=?", req.Name, id); err != nil {
			return err
		}
	}
	raw, err := token()
	if err != nil {
		return err
	}
	turnUser, err := token()
	if err != nil {
		return err
	}
	turnPass, err := token()
	if err != nil {
		return err
	}
	if old := s.peers[id]; old != nil {
		delete(s.sessions, old.tokenHash)
		delete(s.turnUsers, old.turnUser)
	}
	p := &session{uuid: id, tokenHash: digest(raw), turnUser: turnUser, turnPassword: turnPass, expires: now.Add(s.cfg.SessionTTL), lastSeen: now, inbox: []queuedSignal{}}
	s.sessions[p.tokenHash] = p
	s.peers[id] = p
	s.turnUsers[turnUser] = p
	ice := []model.ICEServer{}
	if len(s.cfg.STUNURLs) > 0 {
		ice = append(ice, model.ICEServer{URLs: s.cfg.STUNURLs})
	}
	if s.turn != nil {
		ice = append(ice, model.ICEServer{URLs: []string{s.turn.url}, Username: turnUser, Credential: turnPass})
	}
	seconds := int(s.cfg.HeartbeatInterval / time.Second)
	if seconds < 1 {
		seconds = 1
	}
	return c.JSON(200, model.RegisterResponse{Identity: model.Identity{UUID: id, DeviceSecret: secret}, Token: raw, ICEServers: ice, HeartbeatSeconds: seconds})
}
func (s *Server) heartbeat(c *echo.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	p, err := s.currentLocked(c)
	if err != nil {
		return err
	}
	p.lastSeen = s.cfg.Now()
	return c.JSON(200, map[string]int64{"last_seen": p.lastSeen.Unix(), "expires_at": p.expires.Unix()})
}
func (s *Server) publish(c *echo.Context) error {
	var m model.Manifest
	if err := s.decode(c, &m); err != nil {
		return err
	}
	if err := codec.ValidateManifest(m); err != nil {
		return bad(400, "invalid manifest: "+err.Error())
	}
	body, err := json.Marshal(m)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, err = s.currentLocked(c); err != nil {
		return err
	}
	var existing []byte
	err = s.db.QueryRow("SELECT body FROM manifests WHERE id=?", m.ID).Scan(&existing)
	if err == nil {
		if string(existing) != string(body) {
			return bad(409, "manifest is immutable")
		}
		return c.NoContent(204)
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	var count int
	if err = s.db.QueryRow("SELECT COUNT(*) FROM manifests").Scan(&count); err != nil {
		return err
	}
	if count >= s.cfg.MaxFiles {
		return bad(503, "manifest capacity reached")
	}
	if _, err = s.db.Exec("INSERT INTO manifests(id,body) VALUES(?,?)", m.ID, body); err != nil {
		return err
	}
	return c.NoContent(201)
}
func (s *Server) holdings(c *echo.Context) error {
	var req model.HoldingsRequest
	if err := s.decode(c, &req); err != nil {
		return err
	}
	seen := map[int]bool{}
	for _, i := range req.Indices {
		if i < 0 || i >= model.TotalShards || seen[i] {
			return bad(400, "shard indices must be unique and in range")
		}
		seen[i] = true
	}
	if len(req.FileID) != 64 {
		return bad(400, "invalid file ID")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	p, err := s.currentLocked(c)
	if err != nil {
		return err
	}
	var exists int
	if err = s.db.QueryRow("SELECT 1 FROM manifests WHERE id=?", req.FileID).Scan(&exists); errors.Is(err, sql.ErrNoRows) {
		return bad(404, "file not found")
	} else if err != nil {
		return err
	}
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err = tx.Exec("DELETE FROM holdings WHERE file_id=? AND peer_id=?", req.FileID, p.uuid); err != nil {
		return err
	}
	for _, i := range req.Indices {
		if _, err = tx.Exec("INSERT INTO holdings(file_id,peer_id,shard_index,session_hash) VALUES(?,?,?,?)", req.FileID, p.uuid, i, p.tokenHash); err != nil {
			return err
		}
	}
	if err = tx.Commit(); err != nil {
		return err
	}
	return c.NoContent(204)
}
func (s *Server) infoLocked(m model.Manifest) (model.FileInfo, error) {
	f := model.FileInfo{Manifest: m, Sources: []model.Source{}}
	rows, err := s.db.Query("SELECT peer_id,shard_index,session_hash FROM holdings WHERE file_id=? ORDER BY peer_id,shard_index", m.ID)
	if err != nil {
		return f, err
	}
	defer rows.Close()
	byPeer := map[string]*model.Source{}
	now := s.cfg.Now()
	for rows.Next() {
		var id, hash string
		var idx int
		if err = rows.Scan(&id, &idx, &hash); err != nil {
			return f, err
		}
		p := s.peers[id]
		if p == nil || p.tokenHash != hash || !now.Before(p.expires) || now.Sub(p.lastSeen) >= s.cfg.OnlineTTL {
			continue
		}
		src := byPeer[id]
		if src == nil {
			src = &model.Source{PeerID: id, LastSeen: p.lastSeen.Unix()}
			byPeer[id] = src
		}
		src.Indices = append(src.Indices, idx)
	}
	if err = rows.Err(); err != nil {
		return f, err
	}
	for _, src := range byPeer {
		f.Sources = append(f.Sources, *src)
	}
	sort.Slice(f.Sources, func(i, j int) bool { return f.Sources[i].PeerID < f.Sources[j].PeerID })
	return f, nil
}
func (s *Server) files(c *echo.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, err := s.currentLocked(c); err != nil {
		return err
	}
	rows, err := s.db.Query("SELECT body FROM manifests ORDER BY id")
	if err != nil {
		return err
	}
	manifests := []model.Manifest{}
	for rows.Next() {
		var b []byte
		var m model.Manifest
		if err = rows.Scan(&b); err != nil {
			rows.Close()
			return err
		}
		if err = json.Unmarshal(b, &m); err != nil {
			rows.Close()
			return err
		}
		manifests = append(manifests, m)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	out := make([]model.FileInfo, 0, len(manifests))
	for _, m := range manifests {
		f, err := s.infoLocked(m)
		if err != nil {
			return err
		}
		out = append(out, f)
	}
	return c.JSON(200, out)
}
func (s *Server) file(c *echo.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, err := s.currentLocked(c); err != nil {
		return err
	}
	var b []byte
	err := s.db.QueryRow("SELECT body FROM manifests WHERE id=?", c.Param("id")).Scan(&b)
	if errors.Is(err, sql.ErrNoRows) {
		return bad(404, "file not found")
	}
	if err != nil {
		return err
	}
	var m model.Manifest
	if err = json.Unmarshal(b, &m); err != nil {
		return err
	}
	f, err := s.infoLocked(m)
	if err != nil {
		return err
	}
	return c.JSON(200, f)
}
func (s *Server) sendSignal(c *echo.Context) error {
	var sig model.Signal
	if err := s.decode(c, &sig); err != nil {
		return err
	}
	if len(sig.ID) < 1 || len(sig.ID) > 128 || len(sig.To) != 36 || (sig.Type != "offer" && sig.Type != "answer") || len(sig.SDP) == 0 || len(sig.SDP) > 48<<10 {
		return bad(400, "invalid signal")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	p, err := s.currentLocked(c)
	if err != nil {
		return err
	}
	now := s.cfg.Now()
	target := s.peers[sig.To]
	if target == nil || !now.Before(target.expires) || now.Sub(target.lastSeen) >= s.cfg.OnlineTTL {
		return bad(404, "target peer offline")
	}
	if target == p {
		return bad(400, "cannot signal yourself")
	}
	target.inbox = prune(target.inbox, now)
	if len(target.inbox) >= s.cfg.MaxSignals {
		return bad(429, "target signal queue full")
	}
	sig.From = p.uuid
	target.inbox = append(target.inbox, queuedSignal{signal: sig, expires: now.Add(s.cfg.SignalTTL), fromHash: p.tokenHash})
	return c.NoContent(202)
}
func (s *Server) receiveSignals(c *echo.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	p, err := s.currentLocked(c)
	if err != nil {
		return err
	}
	p.inbox = prune(p.inbox, s.cfg.Now())
	out := make([]model.Signal, 0, len(p.inbox))
	for _, q := range p.inbox {
		from := s.peers[q.signal.From]
		if from == nil || from.tokenHash != q.fromHash || !s.liveLocked(from) {
			continue
		}
		out = append(out, q.signal)
	}
	p.inbox = nil
	return c.JSON(200, out)
}
