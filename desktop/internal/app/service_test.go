package app

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Paulkm2006/p2pfs/internal/client"
	"github.com/Paulkm2006/p2pfs/internal/model"
)

type fakeBackend struct {
	started, closed bool
	startError      error
	files           []model.FileInfo
	tasks           []client.Task
	publishStarted  chan struct{}
	action          string
}

func (f *fakeBackend) Start(context.Context) error { f.started = true; return f.startError }
func (f *fakeBackend) Close() error                { f.closed = true; return nil }
func (f *fakeBackend) Identity() model.Identity {
	return model.Identity{UUID: "peer-1", DeviceSecret: "must-not-leak"}
}
func (f *fakeBackend) Status() string                                      { return "已连接" }
func (f *fakeBackend) LocalFiles() []client.LocalFile                      { return nil }
func (f *fakeBackend) ListFiles(context.Context) ([]model.FileInfo, error) { return f.files, nil }
func (f *fakeBackend) Publish(ctx context.Context, _ string) (model.Manifest, error) {
	if f.publishStarted != nil {
		close(f.publishStarted)
		<-ctx.Done()
		return model.Manifest{}, ctx.Err()
	}
	return model.Manifest{}, nil
}
func (f *fakeBackend) StartDownload(_, path string) (string, error) {
	f.tasks = append(f.tasks, client.Task{ID: "task", Destination: path})
	return "task", nil
}
func (f *fakeBackend) Tasks() []client.Task                        { return f.tasks }
func (f *fakeBackend) Pause(string) error                          { f.action = "pause"; return nil }
func (f *fakeBackend) Resume(string) error                         { f.action = "resume"; return nil }
func (f *fakeBackend) Cancel(string) error                         { f.action = "cancel"; return nil }
func (f *fakeBackend) Retain(context.Context, string, []int) error { f.action = "retain"; return nil }
func (f *fakeBackend) Unshare(context.Context, string, bool) error { f.action = "unshare"; return nil }

func fixture(t *testing.T) (*Service, *fakeBackend, Config) {
	t.Helper()
	dir := t.TempDir()
	s, err := New(filepath.Join(dir, "desktop.json"))
	if err != nil {
		t.Fatal(err)
	}
	f := &fakeBackend{}
	s.factory = func(client.Config) (backend, error) { return f, nil }
	cfg := Config{ServerURL: "https://example.test", DataDir: filepath.Join(dir, "data"), Name: "设备 A", Concurrency: 3}
	return s, f, cfg
}
func TestConnectPersistsOnlyPublicConfig(t *testing.T) {
	s, f, cfg := fixture(t)
	if err := s.Connect(cfg); err != nil {
		t.Fatal(err)
	}
	defer s.Disconnect()
	if !f.started || !s.Status().Connected {
		t.Fatal("not connected")
	}
	raw, _ := json.Marshal(s.Status())
	if strings.Contains(string(raw), "must-not-leak") || strings.Contains(string(raw), "device_secret") {
		t.Fatal("secret exposed")
	}
	data, err := os.ReadFile(s.configPath)
	if err != nil {
		t.Fatal(err)
	}
	var got Config
	if err = json.Unmarshal(data, &got); err != nil || got != cfg {
		t.Fatalf("config: %s %v", data, err)
	}
	if err = s.Connect(cfg); err == nil {
		t.Fatal("duplicate connect accepted")
	}
}
func TestFailedConnectClosesBackend(t *testing.T) {
	s, f, cfg := fixture(t)
	f.startError = errors.New("offline")
	if err := s.Connect(cfg); err == nil {
		t.Fatal("wanted error")
	}
	if !f.closed || s.Status().Connected {
		t.Fatal("failed connection leaked")
	}
}
func TestFilesFilterNameAndVersion(t *testing.T) {
	s, f, cfg := fixture(t)
	f.files = []model.FileInfo{{Manifest: model.Manifest{Name: "Report.pdf", ID: "abc123"}}, {Manifest: model.Manifest{Name: "notes.txt", ID: "DEF789"}}}
	if err := s.Connect(cfg); err != nil {
		t.Fatal(err)
	}
	defer s.Disconnect()
	for q, want := range map[string]string{"REPORT": "abc123", "def": "DEF789"} {
		got, err := s.Files(q)
		if err != nil || len(got) != 1 || got[0].Manifest.ID != want {
			t.Fatalf("%s: %+v %v", q, got, err)
		}
	}
}
func TestDownloadRejectsOverwriteAndDuplicate(t *testing.T) {
	s, _, cfg := fixture(t)
	if err := s.Connect(cfg); err != nil {
		t.Fatal(err)
	}
	defer s.Disconnect()
	target := filepath.Join(t.TempDir(), "out.bin")
	if err := os.WriteFile(target, []byte("keep"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Download("file", target); err == nil {
		t.Fatal("overwrite accepted")
	}
	newPath := target + ".new"
	if _, err := s.Download("file", newPath); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Download("file", newPath); err == nil {
		t.Fatal("duplicate accepted")
	}
	data, _ := os.ReadFile(target)
	if string(data) != "keep" {
		t.Fatal("existing file changed")
	}
}
func TestRetainAndTaskActionsValidate(t *testing.T) {
	s, f, cfg := fixture(t)
	if err := s.Connect(cfg); err != nil {
		t.Fatal(err)
	}
	defer s.Disconnect()
	for _, indices := range [][]int{nil, {-1}, {6}, {1, 1}} {
		if err := s.Retain("f", indices); err == nil {
			t.Fatalf("accepted %v", indices)
		}
	}
	if err := s.Retain("f", []int{0, 3, 5}); err != nil || f.action != "retain" {
		t.Fatal(err)
	}
	if err := s.TaskAction("t", "unknown"); err == nil {
		t.Fatal("invalid action")
	}
	if err := s.TaskAction("t", "retry"); err != nil || f.action != "resume" {
		t.Fatal(err)
	}
}
func TestStorageCountsActualRegularFiles(t *testing.T) {
	s, _, cfg := fixture(t)
	s.config = cfg
	if err := os.MkdirAll(filepath.Join(cfg.DataDir, "cache"), 0700); err != nil {
		t.Fatal(err)
	}
	os.WriteFile(filepath.Join(cfg.DataDir, "config.json"), []byte("123"), 0600)
	os.WriteFile(filepath.Join(cfg.DataDir, "cache", "shard"), []byte("45678"), 0600)
	out, err := s.Storage()
	if err != nil || out.Bytes != 8 || out.Files != 2 {
		t.Fatalf("%+v %v", out, err)
	}
}
func TestDisconnectCancelsOngoingPublish(t *testing.T) {
	s, f, cfg := fixture(t)
	f.publishStarted = make(chan struct{})
	if err := s.Connect(cfg); err != nil {
		t.Fatal(err)
	}
	published := make(chan error, 1)
	go func() { _, err := s.Publish("unused"); published <- err }()
	<-f.publishStarted
	done := make(chan error, 1)
	go func() { done <- s.Disconnect() }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("disconnect blocked behind publish")
	}
	if err := <-published; !errors.Is(err, context.Canceled) {
		t.Fatalf("publish: %v", err)
	}
}
func TestDisconnectedActionsFail(t *testing.T) {
	s, _, _ := fixture(t)
	if _, err := s.Files(""); err == nil {
		t.Fatal("files")
	}
	if _, err := s.Download("f", "x"); err == nil {
		t.Fatal("download")
	}
	if err := s.TaskAction("t", "pause"); err == nil {
		t.Fatal("pause")
	}
}

func TestStatisticsExportRedactsCredentialsAndRejectsOverwrite(t *testing.T) {
	s, _, cfg := fixture(t)
	if err := s.Connect(cfg); err != nil {
		t.Fatal(err)
	}
	defer s.Disconnect()
	path := filepath.Join(t.TempDir(), "stats.json")
	if err := s.ExportStatistics(path); err != nil {
		t.Fatal(err)
	}
	data, _ := os.ReadFile(path)
	if strings.Contains(string(data), "must-not-leak") || strings.Contains(string(data), "secret") || !strings.Contains(string(data), "peer-1") {
		t.Fatalf("invalid export: %s", data)
	}
	if err := s.ExportStatistics(path); err == nil {
		t.Fatal("export overwrote existing file")
	}
}
