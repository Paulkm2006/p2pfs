package codec

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Paulkm2006/p2pfs/internal/model"
)

// TestLargeFileRoundTrip is opt-in because it writes several GiB. Compile a
// test binary, then measure that binary rather than go test's build subprocess:
//
//	go test -c -o /tmp/p2pfs-codec.test ./internal/codec
//	P2PFS_LARGE_TEST=1 /usr/bin/time -v /tmp/p2pfs-codec.test -test.run '^TestLargeFileRoundTrip$' -test.v
//
// This is a storage-only RSS measurement, not the full-client resource claim.
func TestLargeFileRoundTrip(t *testing.T) {
	if os.Getenv("P2PFS_LARGE_TEST") != "1" {
		t.Skip("set P2PFS_LARGE_TEST=1 for the 1 GiB storage resource test")
	}
	root := t.TempDir()
	source := filepath.Join(root, "one-gib.bin")
	cache := filepath.Join(root, "cache")
	f, err := os.Create(source)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.Truncate(model.MaxFileSize); err != nil {
		f.Close()
		t.Fatal(err)
	}
	// Sparse zeros plus nonzero markers at the start, every data-shard boundary,
	// and the end exercise ordering/padding without allocating an input-sized slice.
	for _, offset := range []int64{0, model.MaxFileSize / 4, model.MaxFileSize / 2, 3 * model.MaxFileSize / 4, model.MaxFileSize - 4} {
		if _, err := f.WriteAt([]byte{0x01, 0x25, 0xa9, 0xff}, offset); err != nil {
			f.Close()
			t.Fatal(err)
		}
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	m, err := Encode(context.Background(), source, cache)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("encode 1 GiB: %s", time.Since(start))
	for _, index := range []int{1, 3} {
		if err := os.Remove(ShardPath(cache, m.ID, index)); err != nil {
			t.Fatal(err)
		}
	}
	out := filepath.Join(root, "restored.bin")
	start = time.Now()
	if err := Reconstruct(context.Background(), m, cache, out); err != nil {
		t.Fatal(err)
	}
	t.Logf("restore from 0,2,4,5: %s", time.Since(start))
	result, err := os.Open(out)
	if err != nil {
		t.Fatal(err)
	}
	defer result.Close()
	sha, size, err := digest(context.Background(), result)
	if err != nil || size != model.MaxFileSize || sha != m.SHA256 {
		t.Fatalf("restored digest/size mismatch: %s, %d, %v", sha, size, err)
	}
}
