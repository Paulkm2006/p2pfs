package client

import (
	"os"
	"path/filepath"
	"testing"
)

func TestExclusiveDirectoryAndIdempotentClose(t *testing.T) {
	cfg := Config{ServerURL: "https://example.com", DataDir: t.TempDir()}
	first, e := New(cfg)
	if e != nil {
		t.Fatal(e)
	}
	if other, e := New(cfg); e == nil {
		other.Close()
		t.Fatal("two clients acquired same identity directory")
	}
	if e = first.Close(); e != nil {
		t.Fatal(e)
	}
	second, e := New(cfg)
	if e != nil {
		t.Fatal(e)
	}
	defer second.Close()
	second.mu.Lock()
	second.state.Identity.UUID = "new-state"
	e = second.saveLocked()
	second.mu.Unlock()
	if e != nil {
		t.Fatal(e)
	}
	before, e := os.ReadFile(filepath.Join(cfg.DataDir, "config.json"))
	if e != nil {
		t.Fatal(e)
	}
	if e = first.Close(); e != nil {
		t.Fatal(e)
	}
	after, e := os.ReadFile(filepath.Join(cfg.DataDir, "config.json"))
	if e != nil || string(before) != string(after) {
		t.Fatal("double close rewrote newer state")
	}
}
