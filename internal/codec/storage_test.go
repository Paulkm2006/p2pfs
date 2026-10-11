package codec

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/Paulkm2006/p2pfs/internal/model"
)

func fixture(t *testing.T, size int) (model.Manifest, string, []byte) {
	t.Helper()
	root := t.TempDir()
	path := filepath.Join(root, "中文 sample.bin")
	cache := filepath.Join(root, "cache")
	data := make([]byte, size)
	for i := range data {
		data[i] = byte((i*113 + i/7) % 251)
	}
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	m, err := Encode(context.Background(), path, cache)
	if err != nil {
		t.Fatal(err)
	}
	if err := ValidateManifest(m); err != nil {
		t.Fatal(err)
	}
	for i := range m.Shards {
		if err := VerifyShard(m, i, ShardPath(cache, m.ID, i)); err != nil {
			t.Fatal(err)
		}
	}
	return m, cache, data
}

func subsetCache(t *testing.T, m model.Manifest, source string, indices ...int) string {
	t.Helper()
	cache := t.TempDir()
	if err := os.Mkdir(filepath.Join(cache, m.ID), 0700); err != nil {
		t.Fatal(err)
	}
	for _, i := range indices {
		b, err := os.ReadFile(ShardPath(source, m.ID, i))
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(ShardPath(cache, m.ID, i), b, 0600); err != nil {
			t.Fatal(err)
		}
	}
	return cache
}

// AT-02: all C(6,4)=15 subsets, including the empty-file padding scheme,
// one-byte input, non-multiple-of-four sizes, and multiple streaming blocks.
func TestEveryFourShardSubset(t *testing.T) {
	for _, size := range []int{0, 1, 17, 4*streamBlockSize + 13} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			m, source, data := fixture(t, size)
			combinations := 0
			for a := 0; a < 3; a++ {
				for b := a + 1; b < 4; b++ {
					for c := b + 1; c < 5; c++ {
						for d := c + 1; d < 6; d++ {
							indices := []int{a, b, c, d}
							cache := subsetCache(t, m, source, indices...)
							destination := filepath.Join(t.TempDir(), "result")
							if got := Existing(m, cache); !reflect.DeepEqual(got, indices) {
								t.Fatalf("Existing = %v, want %v", got, indices)
							}
							if err := Reconstruct(context.Background(), m, cache, destination); err != nil {
								t.Fatalf("subset %v: %v", indices, err)
							}
							got, err := os.ReadFile(destination)
							if err != nil {
								t.Fatal(err)
							}
							if !bytes.Equal(got, data) {
								t.Fatalf("subset %v produced different content", indices)
							}
							combinations++
						}
					}
				}
			}
			if combinations != 15 {
				t.Fatalf("tested %d combinations", combinations)
			}
		})
	}
}

func TestCorruptAndPartialShardsDoNotCount(t *testing.T) {
	m, source, data := fixture(t, 4099)
	cache := subsetCache(t, m, source, 0, 1, 2, 3, 4, 5)
	if err := os.WriteFile(ShardPath(cache, m.ID, 0), bytes.Repeat([]byte{0xff}, int(m.Shards[0].Size)), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Truncate(ShardPath(cache, m.ID, 1), m.Shards[1].Size-1); err != nil {
		t.Fatal(err)
	}
	for _, i := range []int{0, 1} {
		if err := VerifyShard(m, i, ShardPath(cache, m.ID, i)); !errors.Is(err, ErrShardCorrupt) {
			t.Fatalf("corrupt shard %d: %v", i, err)
		}
	}
	if got := Existing(m, cache); !reflect.DeepEqual(got, []int{2, 3, 4, 5}) {
		t.Fatalf("Existing = %v", got)
	}
	out := filepath.Join(t.TempDir(), "output")
	if err := Reconstruct(context.Background(), m, cache, out); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(out)
	if err != nil || !bytes.Equal(got, data) {
		t.Fatalf("reconstructed incorrect bytes: %v", err)
	}
	if err := os.Remove(ShardPath(cache, m.ID, 2)); err != nil {
		t.Fatal(err)
	}
	out = filepath.Join(t.TempDir(), "insufficient")
	if err := Reconstruct(context.Background(), m, cache, out); !errors.Is(err, ErrInsufficientShards) {
		t.Fatalf("three shards: %v", err)
	}
	if _, err := os.Stat(out); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("failed reconstruction published output: %v", err)
	}
}

func cloneManifest(m model.Manifest) model.Manifest {
	m.Shards = append([]model.Shard(nil), m.Shards...)
	return m
}

func TestManifestValidation(t *testing.T) {
	base, _, _ := fixture(t, 101)
	mutations := map[string]func(*model.Manifest){
		"format":              func(m *model.Manifest) { m.Format++ },
		"codec":               func(m *model.Manifest) { m.Codec = "arbitrary-codec" },
		"negative":            func(m *model.Manifest) { m.Size = -1 },
		"oversize":            func(m *model.Manifest) { m.Size = model.MaxFileSize + 1 },
		"k":                   func(m *model.Manifest) { m.DataShards = 3 },
		"m":                   func(m *model.Manifest) { m.ParityShards = 3 },
		"missing digest":      func(m *model.Manifest) { m.SHA256 = "" },
		"uppercase digest":    func(m *model.Manifest) { m.SHA256 = strings.ToUpper(m.SHA256) },
		"wrong hash encoding": func(m *model.Manifest) { m.SHA256 = strings.Repeat("z", 64) },
		"short shard list":    func(m *model.Manifest) { m.Shards = m.Shards[:5] },
		"duplicate index":     func(m *model.Manifest) { m.Shards[1].Index = 0 },
		"index outside range": func(m *model.Manifest) { m.Shards[0].Index = -1 },
		"shard size":          func(m *model.Manifest) { m.Shards[0].Size++ },
		"shard digest":        func(m *model.Manifest) { m.Shards[0].SHA256 = "" },
	}
	for name, mutation := range mutations {
		t.Run(name, func(t *testing.T) {
			m := cloneManifest(base)
			mutation(&m)
			m.ID = ManifestID(m)
			if err := ValidateManifest(m); !errors.Is(err, ErrInvalidManifest) {
				t.Fatalf("accepted bad manifest: %v", err)
			}
		})
	}
	for _, name := range []string{"", ".", "..", "../escape", "..\\escape", "/absolute", "C:\\secret", "NUL", "con.txt", "COM1.txt", "LPT².bin", "trailing.", "trailing ", "with\nnewline", "file:stream", "bad\x00name", strings.Repeat("a", 256)} {
		t.Run("name "+name, func(t *testing.T) {
			m := cloneManifest(base)
			m.Name = name
			m.ID = ManifestID(m)
			if ValidateManifest(m) == nil {
				t.Fatalf("accepted unsafe name %q", name)
			}
		})
	}
	changed := cloneManifest(base)
	changed.Name = "different.bin"
	if ValidateManifest(changed) == nil {
		t.Fatal("changing display name without ID was accepted")
	}
	changed.ID = ManifestID(changed)
	if changed.ID == base.ID || ValidateManifest(changed) != nil {
		t.Fatal("canonical new ID failed")
	}
	for _, id := range []string{"../escape", strings.Repeat("A", 64), "", strings.Repeat("0", 63)} {
		if ShardPath("cache", id, 0) != "" {
			t.Fatalf("unsafe ID %q yielded path", id)
		}
	}
	for _, index := range []int{-1, 6, 100000} {
		if ShardPath("cache", base.ID, index) != "" {
			t.Fatalf("unsafe index %d yielded path", index)
		}
		if VerifyShard(base, index, "") == nil {
			t.Fatal("invalid index accepted")
		}
	}
}

func TestDeterministicEncodingAndVersionIsolation(t *testing.T) {
	m, cache, data := fixture(t, 1031)
	source := filepath.Join(filepath.Dir(cache), m.Name)
	second, err := Encode(context.Background(), source, cache)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(m, second) {
		t.Fatal("same file generated different manifest")
	}
	data[0] ^= 0xff
	if err := os.WriteFile(source, data, 0600); err != nil {
		t.Fatal(err)
	}
	different, err := Encode(context.Background(), source, cache)
	if err != nil {
		t.Fatal(err)
	}
	if different.ID == m.ID {
		t.Fatal("same-name, different-content files have equal version IDs")
	}
	if err := VerifyShard(m, 0, ShardPath(cache, different.ID, 0)); !errors.Is(err, ErrShardCorrupt) {
		t.Fatalf("cross-version shard accepted: %v", err)
	}
	// An existing corrupt entry is never silently overwritten during re-encode.
	if err := os.WriteFile(ShardPath(cache, different.ID, 0), []byte("partial"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := Encode(context.Background(), source, cache); !errors.Is(err, ErrShardCorrupt) {
		t.Fatalf("corrupt cache conflict: %v", err)
	}
}

func TestNoOverwriteAndFinalDigestCheck(t *testing.T) {
	m, cache, _ := fixture(t, 1023)
	out := filepath.Join(t.TempDir(), "output")
	original := []byte("must remain untouched")
	if err := os.WriteFile(out, original, 0600); err != nil {
		t.Fatal(err)
	}
	if err := Reconstruct(context.Background(), m, cache, out); !errors.Is(err, ErrDestinationExists) {
		t.Fatalf("overwrite: %v", err)
	}
	got, err := os.ReadFile(out)
	if err != nil || !bytes.Equal(got, original) {
		t.Fatalf("existing target changed: %v", err)
	}
	forged := cloneManifest(m)
	sum := sha256.Sum256([]byte("different"))
	forged.SHA256 = hex.EncodeToString(sum[:])
	forged.ID = ManifestID(forged)
	if err := os.Rename(filepath.Join(cache, m.ID), filepath.Join(cache, forged.ID)); err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	out = filepath.Join(root, "bad-result")
	if err := Reconstruct(context.Background(), forged, cache, out); !errors.Is(err, ErrShardCorrupt) {
		t.Fatalf("final digest mismatch: %v", err)
	}
	entries, err := os.ReadDir(root)
	if err != nil || len(entries) != 0 {
		t.Fatalf("failed output or temporary leaked: %v %v", entries, err)
	}
}

type hookContext struct {
	context.Context
	calls   int
	trigger int
	hook    func()
}

func (c *hookContext) Err() error {
	c.calls++
	if c.calls == c.trigger {
		c.hook()
	}
	return c.Context.Err()
}

func TestCancellationAndSourceMutation(t *testing.T) {
	m, cache, _ := fixture(t, 4*streamBlockSize+9)
	source := filepath.Join(filepath.Dir(cache), m.Name)
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := Encode(canceled, source, t.TempDir()); !errors.Is(err, context.Canceled) {
		t.Fatalf("encode cancellation: %v", err)
	}
	if err := Reconstruct(canceled, m, cache, filepath.Join(t.TempDir(), "output")); !errors.Is(err, context.Canceled) {
		t.Fatalf("restore cancellation: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	mid := &hookContext{Context: ctx, trigger: 4, hook: cancel}
	interruptedCache := t.TempDir()
	if _, err := Encode(mid, source, interruptedCache); err == nil {
		t.Fatal("mid-stream cancellation ignored")
	}
	entries, err := os.ReadDir(interruptedCache)
	if err != nil || len(entries) != 0 {
		t.Fatalf("canceled encode leaked partials: %v %v", entries, err)
	}
	before, err := os.Stat(source)
	if err != nil {
		t.Fatal(err)
	}
	mutation := &hookContext{Context: context.Background(), trigger: 4, hook: func() {
		f, err := os.OpenFile(source, os.O_WRONLY, 0600)
		if err != nil {
			t.Fatal(err)
		}
		defer f.Close()
		if _, err := f.WriteAt([]byte("mutated"), 0); err != nil {
			t.Fatal(err)
		}
		// Preserve size and mtime to prove the independent second hash detects
		// changes that metadata-only checks would miss.
		if err := os.Chtimes(source, before.ModTime(), before.ModTime()); err != nil {
			t.Fatal(err)
		}
	}}
	if _, err := Encode(mutation, source, t.TempDir()); !errors.Is(err, ErrSourceChanged) {
		t.Fatalf("mutated source: %v", err)
	}
}

func TestConcurrentDestinationCreationDoesNotOverwrite(t *testing.T) {
	m, cache, _ := fixture(t, 1023)
	baseline := &hookContext{Context: context.Background()}
	if err := Reconstruct(baseline, m, cache, filepath.Join(t.TempDir(), "baseline")); err != nil {
		t.Fatal(err)
	}
	out := filepath.Join(t.TempDir(), "raced-destination")
	winner := []byte("written by another process")
	race := &hookContext{Context: context.Background(), trigger: baseline.calls, hook: func() {
		if err := os.WriteFile(out, winner, 0600); err != nil {
			t.Fatal(err)
		}
	}}
	if err := Reconstruct(race, m, cache, out); !errors.Is(err, ErrDestinationExists) {
		t.Fatalf("concurrently created destination was not protected: %v", err)
	}
	got, err := os.ReadFile(out)
	if err != nil || !bytes.Equal(got, winner) {
		t.Fatalf("racing destination changed: %v", err)
	}
}

func TestEmptyManifestRequiresDeterministicPadding(t *testing.T) {
	m, _, _ := fixture(t, 0)
	sum := sha256.Sum256([]byte{1})
	m.Shards[0].SHA256 = hex.EncodeToString(sum[:])
	m.ID = ManifestID(m)
	if err := ValidateManifest(m); !errors.Is(err, ErrInvalidManifest) {
		t.Fatalf("nonzero empty-file padding accepted: %v", err)
	}
}

func TestOversizeAndNonregularSource(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "huge")
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.Truncate(model.MaxFileSize + 1); err != nil {
		f.Close()
		t.Skipf("sparse files unavailable: %v", err)
	}
	f.Close()
	if _, err := Encode(context.Background(), path, filepath.Join(root, "cache")); err == nil {
		t.Fatal("oversize accepted")
	}
	if _, err := Encode(context.Background(), root, filepath.Join(root, "cache")); err == nil {
		t.Fatal("directory accepted")
	}
}

func TestSymlinkedShardAndCacheRejected(t *testing.T) {
	m, source, _ := fixture(t, 43)
	cache := subsetCache(t, m, source, 0, 1, 2)
	if err := os.Symlink(ShardPath(source, m.ID, 3), ShardPath(cache, m.ID, 3)); err != nil {
		t.Skipf("symlink creation unavailable: %v", err)
	}
	if err := VerifyShard(m, 3, ShardPath(cache, m.ID, 3)); !errors.Is(err, ErrShardCorrupt) {
		t.Fatalf("symlinked shard: %v", err)
	}
	if err := Reconstruct(context.Background(), m, cache, filepath.Join(t.TempDir(), "output")); !errors.Is(err, ErrInsufficientShards) {
		t.Fatalf("symlink counted: %v", err)
	}
	linked := t.TempDir()
	if err := os.Symlink(filepath.Join(source, m.ID), filepath.Join(linked, m.ID)); err != nil {
		t.Fatal(err)
	}
	if got := Existing(m, linked); len(got) != 0 {
		t.Fatalf("symlinked cache directory counted: %v", got)
	}
	rootLink := filepath.Join(t.TempDir(), "linked-cache")
	if err := os.Symlink(source, rootLink); err != nil {
		t.Fatal(err)
	}
	if got := Existing(m, rootLink); len(got) != 0 {
		t.Fatalf("symlinked cache root counted: %v", got)
	}
	if err := Reconstruct(context.Background(), m, rootLink, filepath.Join(t.TempDir(), "output")); !errors.Is(err, ErrInsufficientShards) {
		t.Fatalf("symlinked cache root restored: %v", err)
	}
	if _, err := Encode(context.Background(), filepath.Join(filepath.Dir(source), m.Name), rootLink); err == nil {
		t.Fatal("encoding into symlinked cache root allowed")
	}
}
