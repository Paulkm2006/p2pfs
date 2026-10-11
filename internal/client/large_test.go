package client

import (
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// Opt-in resource test: sparse source limits fixture generation, but every byte
// is encoded, sent through real encrypted UDP, reconstructed and hashed.
func TestGiBClientRoundTrip(t *testing.T) {
	if os.Getenv("P2PFS_CLIENT_LARGE") != "1" {
		t.Skip("set P2PFS_CLIENT_LARGE=1; needs several GiB of temporary disk")
	}
	url, _ := testServer(t)
	p, d := testPeer(t, url), testPeer(t, url)
	path := filepath.Join(t.TempDir(), "one-gib.bin")
	f, e := os.Create(path)
	if e != nil {
		t.Fatal(e)
	}
	if e = f.Truncate(1 << 30); e != nil {
		t.Fatal(e)
	}
	for _, offset := range []int64{0, (1 << 28) - 1, 1 << 28, (1 << 29) - 1, 1 << 29, 3 << 28, (1 << 30) - 16} {
		if _, e = f.WriteAt([]byte("p2pfs-marker-v1!"), offset); e != nil {
			t.Fatal(e)
		}
	}
	f.Close()
	start := time.Now()
	m, e := p.Publish(context.Background(), path)
	if e != nil {
		t.Fatal(e)
	}
	encoded := time.Since(start)
	dest := filepath.Join(t.TempDir(), "recovered.bin")
	id, e := d.StartDownload(m.ID, dest)
	if e != nil {
		t.Fatal(e)
	}
	deadline := time.After(5 * time.Minute)
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	var completed Task
loop:
	for {
		for _, task := range d.Tasks() {
			if task.ID == id {
				if task.State == "complete" {
					completed = task
					break loop
				}
				if task.State == "failed" {
					t.Fatal(task.Error)
				}
			}
		}
		select {
		case <-deadline:
			t.Fatal("large download timeout", d.Tasks())
		case <-ticker.C:
		}
	}
	output, e := os.Open(dest)
	if e != nil {
		t.Fatal(e)
	}
	defer output.Close()
	h := sha256.New()
	n, e := io.Copy(h, output)
	if e != nil || n != 1<<30 || fmt.Sprintf("%x", h.Sum(nil)) != m.SHA256 {
		t.Fatal("large output integrity", n, e)
	}
	t.Logf("source_bytes=%d encode=%s end_to_end=%s received_payload=%d transfers=%d", m.Size, encoded, time.Since(start), completed.Bytes, len(completed.Transfers))
}
