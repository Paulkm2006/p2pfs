// Package client coordinates persistent local storage and peer-to-peer transfers.
package client

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/Paulkm2006/p2pfs/internal/codec"
	"github.com/Paulkm2006/p2pfs/internal/model"
	"github.com/Paulkm2006/p2pfs/internal/transport"
)

type Config struct {
	TransportOptions     []transport.Option `json:"-"`
	ServerURL            string             `json:"server_url"`
	DataDir              string             `json:"data_dir"`
	Name                 string             `json:"name"`
	InsecureLocal        bool               `json:"insecure_local"`
	ForceRelay           bool               `json:"force_relay"`
	Concurrency          int                `json:"concurrency"`
	UploadBytesPerSecond int64              `json:"upload_bytes_per_second"`
	FixedScheduler       bool               `json:"fixed_scheduler"`
}

type Task struct {
	ID              string     `json:"id"`
	FileID          string     `json:"file_id"`
	Destination     string     `json:"destination"`
	State           string     `json:"state"`
	Error           string     `json:"error"`
	CompletedShards []int      `json:"completed_shards"`
	Bytes           int64      `json:"bytes"`
	CreatedAt       time.Time  `json:"created_at"`
	Transfers       []Transfer `json:"transfers"`
}
type Transfer struct {
	PeerID       string    `json:"peer_id"`
	Index        int       `json:"index"`
	Path         string    `json:"path"`
	Bytes        int64     `json:"bytes"`
	Milliseconds int64     `json:"milliseconds"`
	StartedAt    time.Time `json:"started_at"`
	Error        string    `json:"error,omitempty"`
}
type localState struct {
	ServerURL string                    `json:"server_url"`
	Identity  model.Identity            `json:"identity"`
	Manifests map[string]model.Manifest `json:"manifests"`
	Sharing   map[string]bool           `json:"sharing"`
	Tasks     map[string]*Task          `json:"tasks"`
}
type runningTask struct {
	cancel context.CancelFunc
	done   chan struct{}
}
type peerScore struct {
	rate     float64
	failures int
	failedAt time.Time
}
type Client struct {
	cfg           Config
	http          *http.Client
	mu            sync.Mutex
	state         localState
	token         string
	transport     *transport.Transport
	ctx           context.Context
	cancel        context.CancelFunc
	wg            sync.WaitGroup
	started       bool
	lastError     string
	pendingSync   bool
	runs          map[string]*runningTask
	scores        map[string]peerScore
	failedChoices map[string]time.Time
	reportMu      sync.Mutex
	publishing    int
	operations    map[string]bool
	transferMu    sync.Mutex // serializes cache publication, not network transfers
}

func New(cfg Config) (*Client, error) {
	u, err := url.Parse(cfg.ServerURL)
	if err != nil || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return nil, errors.New("服务端地址无效")
	}
	if u.Scheme != "https" {
		ip := net.ParseIP(u.Hostname())
		loop := strings.EqualFold(u.Hostname(), "localhost") || (ip != nil && ip.IsLoopback())
		if u.Scheme != "http" || !cfg.InsecureLocal || !loop {
			return nil, errors.New("服务端必须使用 HTTPS；本机测试需显式启用 insecure-local")
		}
	}
	cfg.ServerURL = strings.TrimRight(cfg.ServerURL, "/")
	if cfg.DataDir == "" {
		return nil, errors.New("必须设置本地数据目录")
	}
	cfg.DataDir, err = filepath.Abs(cfg.DataDir)
	if err != nil {
		return nil, err
	}
	if cfg.Concurrency <= 0 {
		cfg.Concurrency = 3
	}
	if cfg.Concurrency > 6 {
		return nil, errors.New("并发上限为 6")
	}
	if err = os.MkdirAll(cfg.DataDir, 0700); err != nil {
		return nil, err
	}
	c := &Client{cfg: cfg, http: &http.Client{Timeout: 15 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}, runs: map[string]*runningTask{}, scores: map[string]peerScore{}, failedChoices: map[string]time.Time{}, operations: map[string]bool{}}
	c.state = localState{ServerURL: cfg.ServerURL, Manifests: map[string]model.Manifest{}, Sharing: map[string]bool{}, Tasks: map[string]*Task{}}
	data, err := os.ReadFile(filepath.Join(cfg.DataDir, "config.json"))
	if err == nil {
		if err = json.Unmarshal(data, &c.state); err != nil {
			return nil, fmt.Errorf("本地配置损坏: %w", err)
		}
		if c.state.ServerURL != cfg.ServerURL {
			return nil, errors.New("该数据目录已绑定其他服务端，请使用独立目录")
		}
	} else if !os.IsNotExist(err) {
		return nil, err
	}
	if c.state.Manifests == nil {
		c.state.Manifests = map[string]model.Manifest{}
	}
	if c.state.Sharing == nil {
		c.state.Sharing = map[string]bool{}
	}
	if c.state.Tasks == nil {
		c.state.Tasks = map[string]*Task{}
	}
	for _, t := range c.state.Tasks {
		if t.State != "complete" && t.State != "cancelled" {
			t.State = "paused"
			t.Error = "客户端已重启，继续时将重新校验完整分片"
		}
	}
	return c, nil
}
func (c *Client) cacheDir() string { return filepath.Join(c.cfg.DataDir, "cache") }
func (c *Client) saveLocked() error {
	b, err := json.MarshalIndent(c.state, "", "  ")
	if err != nil {
		return err
	}
	f, err := os.CreateTemp(c.cfg.DataDir, ".config-*")
	if err != nil {
		return err
	}
	name := f.Name()
	defer os.Remove(name)
	if err = f.Chmod(0600); err == nil {
		_, err = f.Write(b)
	}
	if err == nil {
		err = f.Sync()
	}
	closeErr := f.Close()
	if err == nil {
		err = closeErr
	}
	if err != nil {
		return err
	}
	return os.Rename(name, filepath.Join(c.cfg.DataDir, "config.json"))
}
func (c *Client) Identity() model.Identity {
	c.mu.Lock()
	defer c.mu.Unlock()
	return model.Identity{UUID: c.state.Identity.UUID}
}
func (c *Client) Status() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.lastError != "" {
		return c.lastError
	}
	if c.started {
		return "已连接"
	}
	return "未连接"
}
func (c *Client) Start(ctx context.Context) error {
	c.mu.Lock()
	if c.started {
		c.mu.Unlock()
		return nil
	}
	c.ctx, c.cancel = context.WithCancel(ctx)
	c.started = true
	c.mu.Unlock()
	if err := c.register(c.ctx); err != nil {
		c.mu.Lock()
		c.started = false
		c.cancel()
		t := c.transport
		c.transport = nil
		c.mu.Unlock()
		if t != nil {
			t.Close()
		}
		return err
	}
	c.wg.Add(1)
	go c.controlLoop()
	return nil
}
func (c *Client) register(ctx context.Context) error {
	c.mu.Lock()
	identity := c.state.Identity
	c.mu.Unlock()
	var r model.RegisterResponse
	if err := c.request(ctx, http.MethodPost, "/v1/register", model.RegisterRequest{Identity: identity, Name: c.cfg.Name}, &r); err != nil {
		return err
	}
	if r.UUID == "" || r.Token == "" || r.DeviceSecret == "" {
		return errors.New("服务端未返回完整身份")
	}
	opts := append([]transport.Option{transport.WithForceRelay(c.cfg.ForceRelay), transport.WithUploadBytesPerSecond(c.cfg.UploadBytesPerSecond), transport.WithSessionTimeout(24 * time.Hour)}, c.cfg.TransportOptions...)
	t, err := transport.New(r.UUID, r.ICEServers, c, c.openShard, opts...)
	if err != nil {
		return err
	}
	c.mu.Lock()
	old := c.transport
	c.transport = t
	c.state.Identity = r.Identity
	c.token = r.Token
	c.lastError = ""
	err = c.saveLocked()
	c.mu.Unlock()
	if old != nil {
		old.Close()
	}
	if err != nil {
		t.Close()
		return err
	}
	c.mu.Lock()
	c.pendingSync = true
	c.mu.Unlock()
	if err := c.syncHoldings(ctx); err != nil {
		return err
	}
	c.mu.Lock()
	c.pendingSync = false
	c.mu.Unlock()
	return nil
}
func (c *Client) controlLoop() {
	defer c.wg.Done()
	poll := time.NewTicker(200 * time.Millisecond)
	defer poll.Stop()
	heartbeat := time.NewTicker(5 * time.Second)
	defer heartbeat.Stop()
	for {
		select {
		case <-c.ctx.Done():
			return
		case <-poll.C:
			var signals []model.Signal
			if err := c.request(c.ctx, http.MethodGet, "/v1/signals", nil, &signals); err == nil {
				c.mu.Lock()
				t := c.transport
				c.mu.Unlock()
				for _, s := range signals {
					if t != nil {
						if err = t.HandleSignal(c.ctx, s); err != nil {
							slog.Debug("signal rejected", "error", err)
						}
					}
				}
			} else {
				c.mu.Lock()
				c.lastError = "协调连接异常: " + err.Error()
				c.mu.Unlock()
			}
		case <-heartbeat.C:
			if err := c.request(c.ctx, http.MethodPost, "/v1/heartbeat", struct{}{}, nil); err != nil {
				if err = c.register(c.ctx); err != nil {
					c.mu.Lock()
					c.lastError = "等待协调服务恢复: " + err.Error()
					c.mu.Unlock()
				}
			} else {
				c.mu.Lock()
				pending := c.pendingSync
				c.mu.Unlock()
				var syncErr error
				if pending {
					syncErr = c.syncHoldings(c.ctx)
				}
				c.mu.Lock()
				if syncErr != nil {
					c.lastError = "等待重新上报分片: " + syncErr.Error()
				} else {
					c.pendingSync = false
					c.lastError = ""
				}
				c.mu.Unlock()
			}
		}
	}
}
func (c *Client) Send(ctx context.Context, s model.Signal) error {
	return c.request(ctx, http.MethodPost, "/v1/signals", s, nil)
}
func (c *Client) request(ctx context.Context, method, path string, body, out any) error {
	var reader io.Reader
	if body != nil {
		b, e := json.Marshal(body)
		if e != nil {
			return e
		}
		reader = bytes.NewReader(b)
	}
	req, e := http.NewRequestWithContext(ctx, method, c.cfg.ServerURL+path, reader)
	if e != nil {
		return e
	}
	req.Header.Set("Content-Type", "application/json")
	c.mu.Lock()
	token := c.token
	c.mu.Unlock()
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, e := c.http.Do(req)
	if e != nil {
		return e
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
		return fmt.Errorf("协调 API %d: %s", resp.StatusCode, strings.TrimSpace(string(b)))
	}
	if out != nil {
		return json.NewDecoder(io.LimitReader(resp.Body, 16<<20)).Decode(out)
	}
	return nil
}
func (c *Client) ListFiles(ctx context.Context) ([]model.FileInfo, error) {
	var files []model.FileInfo
	e := c.request(ctx, http.MethodGet, "/v1/files", nil, &files)
	return files, e
}
func (c *Client) file(ctx context.Context, id string) (model.FileInfo, error) {
	var f model.FileInfo
	if len(id) != 64 {
		return f, errors.New("文件版本标识无效")
	}
	_, e := hex.DecodeString(id)
	if e != nil {
		return f, e
	}
	e = c.request(ctx, http.MethodGet, "/v1/files/"+id, nil, &f)
	if e == nil {
		e = codec.ValidateManifest(f.Manifest)
	}
	if e == nil && f.Manifest.ID != id {
		e = errors.New("清单版本不匹配")
	}
	return f, e
}
func (c *Client) Publish(ctx context.Context, path string) (model.Manifest, error) {
	c.mu.Lock()
	c.publishing++
	c.mu.Unlock()
	defer func() { c.mu.Lock(); c.publishing--; c.mu.Unlock() }()
	info, e := os.Stat(path)
	if e != nil {
		return model.Manifest{}, e
	}
	if info.Size() > model.MaxFileSize {
		return model.Manifest{}, errors.New("文件超过 1 GiB 上限")
	}
	free, e := availableBytes(c.cfg.DataDir)
	if e != nil {
		return model.Manifest{}, e
	}
	if free < uint64(max(info.Size(), 4))*3/2+(16<<20) {
		return model.Manifest{}, errors.New("缓存磁盘空间不足，需要文件大小的约 1.5 倍额外空间")
	}
	m, e := codec.Encode(ctx, path, c.cacheDir())
	if e != nil {
		return m, e
	}
	if e = c.beginOperation(m.ID); e != nil {
		return m, e
	}
	defer c.endOperation(m.ID)
	if e = c.request(ctx, http.MethodPost, "/v1/manifests", m, nil); e != nil {
		return m, e
	}
	c.mu.Lock()
	c.state.Manifests[m.ID] = m
	c.state.Sharing[m.ID] = true
	e = c.saveLocked()
	c.mu.Unlock()
	if e != nil {
		return m, e
	}
	return m, c.report(ctx, m)
}
func (c *Client) report(ctx context.Context, m model.Manifest) error {
	c.reportMu.Lock()
	defer c.reportMu.Unlock()
	c.mu.Lock()
	shared := c.state.Sharing[m.ID]
	c.mu.Unlock()
	indices := []int{}
	if shared {
		indices = codec.Existing(m, c.cacheDir())
	}
	return c.request(ctx, http.MethodPut, "/v1/holdings", model.HoldingsRequest{FileID: m.ID, Indices: indices}, nil)
}
func (c *Client) syncHoldings(ctx context.Context) error {
	c.mu.Lock()
	var ms []model.Manifest
	for id, m := range c.state.Manifests {
		if c.state.Sharing[id] {
			ms = append(ms, m)
		}
	}
	c.mu.Unlock()
	for _, m := range ms {
		if err := c.request(ctx, http.MethodPost, "/v1/manifests", m, nil); err != nil {
			return err
		}
		if err := c.report(ctx, m); err != nil {
			return err
		}
	}
	return nil
}
func (c *Client) openShard(id string, index int) (io.ReadCloser, int64, error) {
	c.mu.Lock()
	m, ok := c.state.Manifests[id]
	shared := c.state.Sharing[id]
	c.mu.Unlock()
	if !ok || !shared || index < 0 || index >= model.TotalShards {
		return nil, 0, errors.New("分片未共享")
	}
	path := codec.ShardPath(c.cacheDir(), id, index)
	if err := codec.VerifyShard(m, index, path); err != nil {
		return nil, 0, err
	}
	f, e := os.Open(path)
	return f, m.Shards[index].Size, e
}
func (c *Client) Close() error {
	c.mu.Lock()
	if c.cancel != nil {
		c.cancel()
	}
	for _, run := range c.runs {
		run.cancel()
	}
	t := c.transport
	c.started = false
	c.mu.Unlock()
	if t != nil {
		t.Close()
	}
	c.wg.Wait()
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.saveLocked()
}
func (c *Client) Tasks() []Task {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]Task, 0, len(c.state.Tasks))
	for _, t := range c.state.Tasks {
		v := *t
		v.CompletedShards = append([]int{}, t.CompletedShards...)
		v.Transfers = append([]Transfer{}, t.Transfers...)
		out = append(out, v)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].CreatedAt.Before(out[j].CreatedAt) })
	return out
}
func newID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(err)
	}
	return hex.EncodeToString(b[:])
}
func (c *Client) beginOperation(id string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.operations[id] {
		return errors.New("该文件正在发布或接收分片，请等待完成或取消当前操作")
	}
	c.operations[id] = true
	return nil
}
func (c *Client) endOperation(id string) { c.mu.Lock(); delete(c.operations, id); c.mu.Unlock() }
