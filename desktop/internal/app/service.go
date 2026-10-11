// Package app contains the testable desktop service without GUI dependencies.
package app

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/Paulkm2006/p2pfs/internal/client"
	"github.com/Paulkm2006/p2pfs/internal/model"
)

type Config struct {
	ServerURL            string `json:"server_url"`
	DataDir              string `json:"data_dir"`
	Name                 string `json:"name"`
	InsecureLocal        bool   `json:"insecure_local"`
	ForceRelay           bool   `json:"force_relay"`
	Concurrency          int    `json:"concurrency"`
	UploadBytesPerSecond int64  `json:"upload_bytes_per_second"`
}

// Status intentionally omits the device secret and session token.
type Status struct {
	Connected        bool   `json:"connected"`
	ConnectionStatus string `json:"connection_status"`
	UUID             string `json:"uuid"`
	Config           Config `json:"config"`
	Tasks            []Task `json:"tasks"`
}
type Task struct {
	ID              string            `json:"id"`
	FileID          string            `json:"file_id"`
	Destination     string            `json:"destination"`
	State           string            `json:"state"`
	Error           string            `json:"error"`
	CompletedShards []int             `json:"completed_shards"`
	Bytes           int64             `json:"bytes"`
	CreatedAt       time.Time         `json:"created_at"`
	Transfers       []client.Transfer `json:"transfers"`
}
type Storage struct {
	Path  string `json:"path"`
	Bytes int64  `json:"bytes"`
	Files int    `json:"files"`
}

type backend interface {
	Start(context.Context) error
	Close() error
	Identity() model.Identity
	Status() string
	LocalFiles() []client.LocalFile
	ListFiles(context.Context) ([]model.FileInfo, error)
	Publish(context.Context, string) (model.Manifest, error)
	StartDownload(string, string) (string, error)
	Tasks() []client.Task
	Pause(string) error
	Resume(string) error
	Cancel(string) error
	Retain(context.Context, string, []int) error
	Unshare(context.Context, string, bool) error
}

type Service struct {
	mu         sync.RWMutex
	cancelMu   sync.Mutex
	ctx        context.Context
	configPath string
	config     Config
	client     backend
	cancel     context.CancelFunc
	factory    func(client.Config) (backend, error)
}

func New(configPath string) (*Service, error) {
	home, err := os.UserConfigDir()
	if err != nil {
		return nil, err
	}
	name, _ := os.Hostname()
	if name == "" {
		name = "Windows 设备"
	}
	s := &Service{configPath: configPath, config: Config{ServerURL: "https://localhost:8443", DataDir: filepath.Join(home, "p2pfs", "data"), Name: name, Concurrency: 3}}
	s.factory = func(cfg client.Config) (backend, error) { return client.New(cfg) }
	data, err := os.ReadFile(configPath)
	if err == nil {
		if err = json.Unmarshal(data, &s.config); err != nil {
			return nil, fmt.Errorf("读取桌面配置失败：%w", err)
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	return s, nil
}

func (s *Service) Connect(cfg Config) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.client != nil {
		return errors.New("请先断开当前会话，再修改连接设置")
	}
	cfg.ServerURL = strings.TrimSpace(cfg.ServerURL)
	cfg.Name = strings.TrimSpace(cfg.Name)
	cfg.DataDir = strings.TrimSpace(cfg.DataDir)
	if cfg.ServerURL == "" || cfg.DataDir == "" || cfg.Name == "" {
		return errors.New("请填写服务端、设备名称和本机数据目录")
	}
	abs, err := filepath.Abs(cfg.DataDir)
	if err != nil {
		return err
	}
	cfg.DataDir = abs
	if cfg.Concurrency == 0 {
		cfg.Concurrency = 3
	}
	if cfg.Concurrency < 1 || cfg.Concurrency > 6 || cfg.UploadBytesPerSecond < 0 {
		return errors.New("并发量必须为 1 至 6，上行速率上限不能为负数")
	}
	c, err := s.factory(client.Config{ServerURL: cfg.ServerURL, DataDir: cfg.DataDir, Name: cfg.Name, InsecureLocal: cfg.InsecureLocal, ForceRelay: cfg.ForceRelay, Concurrency: cfg.Concurrency, UploadBytesPerSecond: cfg.UploadBytesPerSecond})
	if err != nil {
		return err
	}
	ctx, cancel := context.WithCancel(context.Background())
	if err = c.Start(ctx); err != nil {
		cancel()
		_ = c.Close()
		return err
	}
	// Persist non-secret UI configuration only. Identity persistence belongs to the client.
	data, err := json.MarshalIndent(cfg, "", "  ")
	if err == nil {
		err = os.MkdirAll(filepath.Dir(s.configPath), 0700)
	}
	if err == nil {
		err = os.WriteFile(s.configPath, data, 0600)
	}
	if err != nil {
		cancel()
		_ = c.Close()
		return fmt.Errorf("保存配置失败：%w", err)
	}
	s.config = cfg
	s.client = c
	s.ctx = ctx
	s.cancelMu.Lock()
	s.cancel = cancel
	s.cancelMu.Unlock()
	return nil
}

func (s *Service) Disconnect() error {
	// Cancel long-running publish/retain calls before waiting for their read locks.
	s.cancelMu.Lock()
	if s.cancel != nil {
		s.cancel()
	}
	s.cancelMu.Unlock()
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.client == nil {
		return nil
	}
	s.cancel()
	err := s.client.Close()
	s.client = nil
	s.cancelMu.Lock()
	s.cancel = nil
	s.cancelMu.Unlock()
	s.ctx = nil
	return err
}
func (s *Service) Status() Status {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := Status{Config: s.config, Tasks: []Task{}}
	if s.client == nil {
		return out
	}
	out.Connected = true
	out.UUID = s.client.Identity().UUID
	out.ConnectionStatus = s.client.Status()
	for _, t := range s.client.Tasks() {
		out.Tasks = append(out.Tasks, Task{ID: t.ID, FileID: t.FileID, Destination: t.Destination, State: t.State, Error: t.Error, CompletedShards: append([]int{}, t.CompletedShards...), Bytes: t.Bytes, CreatedAt: t.CreatedAt, Transfers: append([]client.Transfer{}, t.Transfers...)})
	}
	sort.Slice(out.Tasks, func(i, j int) bool { return out.Tasks[i].CreatedAt.After(out.Tasks[j].CreatedAt) })
	return out
}
func (s *Service) Files(query string) ([]model.FileInfo, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.client == nil {
		return nil, errors.New("请先连接服务端")
	}
	ctx, cancel := context.WithTimeout(s.ctx, 20*time.Second)
	defer cancel()
	files, err := s.client.ListFiles(ctx)
	if err != nil {
		return nil, err
	}
	q := strings.ToLower(strings.TrimSpace(query))
	out := []model.FileInfo{}
	for _, f := range files {
		if q == "" || strings.Contains(strings.ToLower(f.Manifest.Name), q) || strings.Contains(strings.ToLower(f.Manifest.ID), q) {
			out = append(out, f)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Manifest.Name < out[j].Manifest.Name })
	return out, nil
}
func (s *Service) Publish(path string) (model.Manifest, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.client == nil {
		return model.Manifest{}, errors.New("请先连接服务端")
	}
	return s.client.Publish(s.ctx, path)
}
func (s *Service) Download(fileID, destination string) (string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.client == nil {
		return "", errors.New("请先连接服务端")
	}
	if destination == "" {
		return "", errors.New("请选择保存位置")
	}
	// The desktop never silently overwrites an existing file, even after a save dialog.
	if _, err := os.Lstat(destination); err == nil {
		return "", errors.New("目标文件已存在，请选择新的文件名")
	} else if !errors.Is(err, os.ErrNotExist) {
		return "", err
	}
	for _, t := range s.client.Tasks() {
		if t.Destination == destination && t.State != "complete" && t.State != "completed" && t.State != "cancelled" && t.State != "canceled" {
			return "", errors.New("该保存位置已有下载任务，请在任务列表继续或重试")
		}
	}
	return s.client.StartDownload(fileID, destination)
}
func (s *Service) TaskAction(id, action string) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.client == nil {
		return errors.New("请先连接服务端")
	}
	switch action {
	case "pause":
		return s.client.Pause(id)
	case "resume", "retry":
		return s.client.Resume(id)
	case "cancel":
		return s.client.Cancel(id)
	default:
		return errors.New("不支持的任务操作")
	}
}
func (s *Service) Retain(id string, indices []int) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.client == nil {
		return errors.New("请先连接服务端")
	}
	if len(indices) == 0 {
		return errors.New("请至少选择一个分片")
	}
	seen := map[int]bool{}
	for _, i := range indices {
		if i < 0 || i >= model.TotalShards || seen[i] {
			return errors.New("分片索引必须在 0 至 5 之间且不重复")
		}
		seen[i] = true
	}
	return s.client.Retain(s.ctx, id, indices)
}
func (s *Service) Unshare(id string, deleteCache bool) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.client == nil {
		return errors.New("请先连接服务端")
	}
	return s.client.Unshare(s.ctx, id, deleteCache)
}
func (s *Service) Storage() (Storage, error) {
	s.mu.RLock()
	root := s.config.DataDir
	s.mu.RUnlock()
	out := Storage{Path: root}
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if errors.Is(err, os.ErrNotExist) && path == root {
			return nil
		}
		if err != nil {
			return err
		}
		if d.Type().IsRegular() {
			info, err := d.Info()
			if err != nil {
				return err
			}
			out.Files++
			out.Bytes += info.Size()
		}
		return nil
	})
	return out, err
}

func (s *Service) LocalFiles() ([]client.LocalFile, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.client == nil {
		return []client.LocalFile{}, nil
	}
	return s.client.LocalFiles(), nil
}

// ExportStatistics creates a new diagnostics file without serializing credentials.
func (s *Service) ExportStatistics(path string) error {
	status := s.Status()
	payload := struct {
		SchemaVersion int       `json:"schema_version"`
		GeneratedAt   time.Time `json:"generated_at"`
		UUID          string    `json:"device_uuid"`
		Connection    string    `json:"connection_status"`
		Tasks         []Task    `json:"tasks"`
	}{1, time.Now().UTC(), status.UUID, status.ConnectionStatus, status.Tasks}
	data, err := json.MarshalIndent(payload, "", "  ")
	if err != nil {
		return err
	}
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return fmt.Errorf("无法导出，请选择不存在的新文件名：%w", err)
	}
	_, err = f.Write(data)
	closeErr := f.Close()
	if err != nil {
		return err
	}
	return closeErr
}
