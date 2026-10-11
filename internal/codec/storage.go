package codec

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"
	"os"
	"path/filepath"

	"github.com/Paulkm2006/p2pfs/internal/model"
	"github.com/klauspost/reedsolomon"
)

// Stream block size bounds working memory independently of file length. The RS
// engine holds six blocks; copies and hashes use a further 256 KiB buffer.
const streamBlockSize = 256 << 10

type contextReader struct {
	ctx    context.Context
	reader io.Reader
}

func (r contextReader) Read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.reader.Read(p)
}

type zeroReader struct{}

func (zeroReader) Read(p []byte) (int, error) { clear(p); return len(p), nil }

func newEncoder() (reedsolomon.StreamEncoder, error) {
	return reedsolomon.NewStream(model.DataShards, model.ParityShards, reedsolomon.WithStreamBlockSize(streamBlockSize))
}

func digest(ctx context.Context, r io.Reader) (string, int64, error) {
	h := sha256.New()
	n, err := io.CopyBuffer(h, contextReader{ctx, r}, make([]byte, streamBlockSize))
	return hex.EncodeToString(h.Sum(nil)), n, err
}

func sameSource(a, b os.FileInfo) bool {
	return os.SameFile(a, b) && a.Size() == b.Size() && a.Mode() == b.Mode() && a.ModTime().Equal(b.ModTime())
}

// Encode produces six complete verified shards using bounded memory. Source
// bytes are hashed during splitting and independently reread before publication;
// identity, size and modification metadata must also remain unchanged.
func Encode(ctx context.Context, path, cacheDir string) (model.Manifest, error) {
	var empty model.Manifest
	if err := ctx.Err(); err != nil {
		return empty, err
	}
	name := filepath.Base(path)
	if !safeName(name) {
		return empty, fmt.Errorf("%w: unsafe source display name", ErrInvalidManifest)
	}
	source, err := os.Open(path)
	if err != nil {
		return empty, err
	}
	defer source.Close()
	initial, err := source.Stat()
	if err != nil {
		return empty, err
	}
	if !initial.Mode().IsRegular() {
		return empty, errors.New("source must be a regular file")
	}
	if initial.Size() > model.MaxFileSize {
		return empty, errors.New("file exceeds the 1 GiB limit")
	}
	if err := os.MkdirAll(cacheDir, 0700); err != nil {
		return empty, err
	}
	if err := checkCacheDirectory(cacheDir); err != nil {
		return empty, err
	}
	stage, err := os.MkdirTemp(cacheDir, ".encode-")
	if err != nil {
		return empty, err
	}
	defer os.RemoveAll(stage)
	files := make([]*os.File, model.TotalShards)
	defer func() {
		for _, f := range files {
			if f != nil {
				f.Close()
			}
		}
	}()
	hashes := make([]hash.Hash, model.TotalShards)
	writers := make([]io.Writer, model.TotalShards)
	for i := range files {
		files[i], err = os.OpenFile(filepath.Join(stage, fmt.Sprintf("%d.shard", i)), os.O_CREATE|os.O_EXCL|os.O_RDWR, 0600)
		if err != nil {
			return empty, err
		}
		hashes[i] = sha256.New()
		writers[i] = io.MultiWriter(files[i], hashes[i])
	}
	rs, err := newEncoder()
	if err != nil {
		return empty, err
	}
	size := initial.Size()
	padded := shardSize(size) * model.DataShards
	fileHash := sha256.New()
	input := io.MultiReader(io.TeeReader(io.LimitReader(source, size), fileHash), io.LimitReader(zeroReader{}, padded-size))
	if err := rs.Split(contextReader{ctx, input}, writers[:model.DataShards], padded); err != nil {
		return empty, fmt.Errorf("split source: %w", err)
	}
	readers := make([]io.Reader, model.DataShards)
	for i := range readers {
		if _, err := files[i].Seek(0, io.SeekStart); err != nil {
			return empty, err
		}
		readers[i] = contextReader{ctx, files[i]}
	}
	if err := rs.Encode(readers, writers[model.DataShards:]); err != nil {
		return empty, fmt.Errorf("encode parity: %w", err)
	}
	m := model.Manifest{Format: Format, Name: name, Size: size, SHA256: hex.EncodeToString(fileHash.Sum(nil)), DataShards: model.DataShards, ParityShards: model.ParityShards, Codec: Codec, Shards: make([]model.Shard, model.TotalShards)}
	for i, f := range files {
		if err := f.Sync(); err != nil {
			return empty, err
		}
		info, err := f.Stat()
		if err != nil {
			return empty, err
		}
		if info.Size() != shardSize(size) {
			return empty, errors.New("unexpected encoded shard length")
		}
		m.Shards[i] = model.Shard{Index: i, Size: info.Size(), SHA256: hex.EncodeToString(hashes[i].Sum(nil))}
		if err := f.Close(); err != nil {
			return empty, err
		}
		files[i] = nil
	}
	if _, err := source.Seek(0, io.SeekStart); err != nil {
		return empty, err
	}
	checkHash, checkSize, err := digest(ctx, io.LimitReader(source, size+1))
	if err != nil {
		return empty, err
	}
	current, err := source.Stat()
	if err != nil {
		return empty, err
	}
	atPath, err := os.Stat(path)
	if err != nil {
		return empty, fmt.Errorf("%w: %v", ErrSourceChanged, err)
	}
	if checkHash != m.SHA256 || checkSize != size || !sameSource(initial, current) || !sameSource(initial, atPath) {
		return empty, ErrSourceChanged
	}
	m.ID = ManifestID(m)
	if err := ValidateManifest(m); err != nil {
		return empty, err
	}
	if err := ctx.Err(); err != nil {
		return empty, err
	}
	targetDir := filepath.Join(cacheDir, m.ID)
	if err := os.Mkdir(targetDir, 0700); err != nil && !errors.Is(err, os.ErrExist) {
		return empty, err
	}
	if err := checkCacheDirectory(targetDir); err != nil {
		return empty, err
	}
	for i := range m.Shards {
		if err := ctx.Err(); err != nil {
			return empty, err
		}
		final := ShardPath(cacheDir, m.ID, i)
		if err := os.Link(filepath.Join(stage, fmt.Sprintf("%d.shard", i)), final); err != nil {
			if !errors.Is(err, os.ErrExist) {
				return empty, fmt.Errorf("publish shard atomically: %w", err)
			}
			if err := verifyShard(ctx, m, i, final); err != nil {
				return empty, fmt.Errorf("existing cache entry conflicts: %w", err)
			}
		}
	}
	return m, nil
}

func checkCacheDirectory(path string) error {
	info, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return errors.New("cache version path must be a real directory")
	}
	return nil
}

func openRegular(path string) (*os.File, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() {
		return nil, fmt.Errorf("%w: shard is not a regular file", ErrShardCorrupt)
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	actual, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	if !actual.Mode().IsRegular() || !os.SameFile(info, actual) {
		f.Close()
		return nil, fmt.Errorf("%w: shard changed while opening", ErrShardCorrupt)
	}
	return f, nil
}

func verifyOpen(ctx context.Context, m model.Manifest, index int, f *os.File) error {
	info, err := f.Stat()
	if err != nil {
		return err
	}
	if info.Size() != m.Shards[index].Size {
		return fmt.Errorf("%w: shard %d length mismatch", ErrShardCorrupt, index)
	}
	sha, n, err := digest(ctx, io.LimitReader(f, m.Shards[index].Size+1))
	if err != nil {
		return err
	}
	if n != m.Shards[index].Size || sha != m.Shards[index].SHA256 {
		return fmt.Errorf("%w: shard %d digest mismatch", ErrShardCorrupt, index)
	}
	_, err = f.Seek(0, io.SeekStart)
	return err
}

func verifyShard(ctx context.Context, m model.Manifest, index int, path string) error {
	f, err := openRegular(path)
	if err != nil {
		return err
	}
	defer f.Close()
	return verifyOpen(ctx, m, index, f)
}

// VerifyShard rejects partial, corrupt, symlinked, or wrong-version shard files.
func VerifyShard(m model.Manifest, index int, path string) error {
	if err := ValidateManifest(m); err != nil {
		return err
	}
	if index < 0 || index >= len(m.Shards) {
		return fmt.Errorf("%w: index out of range", ErrShardCorrupt)
	}
	return verifyShard(context.Background(), m, index, path)
}

// Existing returns only complete, independently verified cache shard indices.
func Existing(m model.Manifest, cacheDir string) []int {
	indices := make([]int, 0, model.TotalShards)
	if ValidateManifest(m) != nil || checkCacheDirectory(cacheDir) != nil || checkCacheDirectory(filepath.Join(cacheDir, m.ID)) != nil {
		return indices
	}
	for i := range m.Shards {
		if verifyShard(context.Background(), m, i, ShardPath(cacheDir, m.ID, i)) == nil {
			indices = append(indices, i)
		}
	}
	return indices
}

// Reconstruct restores a file from any four valid independent shard indices.
// Invalid cache entries are ignored. The destination is atomically created only
// after the full output digest and length pass; an existing path is never replaced.
func Reconstruct(ctx context.Context, m model.Manifest, cacheDir, destination string) error {
	if err := ValidateManifest(m); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if _, err := os.Lstat(destination); err == nil {
		return ErrDestinationExists
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if err := checkCacheDirectory(cacheDir); err != nil {
		return fmt.Errorf("%w: %v", ErrInsufficientShards, err)
	}
	if err := checkCacheDirectory(filepath.Join(cacheDir, m.ID)); err != nil {
		return fmt.Errorf("%w: %v", ErrInsufficientShards, err)
	}
	files := make([]*os.File, model.TotalShards)
	defer func() {
		for _, f := range files {
			if f != nil {
				f.Close()
			}
		}
	}()
	valid := make([]io.Reader, model.TotalShards)
	count := 0
	for i := range m.Shards {
		if err := ctx.Err(); err != nil {
			return err
		}
		f, err := openRegular(ShardPath(cacheDir, m.ID, i))
		if err != nil {
			continue
		}
		if err := verifyOpen(ctx, m, i, f); err != nil {
			f.Close()
			if ctx.Err() != nil {
				return ctx.Err()
			}
			continue
		}
		files[i] = f
		valid[i] = contextReader{ctx, f}
		count++
	}
	if count < model.DataShards {
		return ErrInsufficientShards
	}
	stage, err := os.MkdirTemp(cacheDir, ".reconstruct-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(stage)
	fill := make([]io.Writer, model.TotalShards)
	missing := make([]int, 0, model.ParityShards)
	for i := range files {
		if files[i] == nil {
			files[i], err = os.OpenFile(filepath.Join(stage, fmt.Sprintf("%d.shard", i)), os.O_CREATE|os.O_EXCL|os.O_RDWR, 0600)
			if err != nil {
				return err
			}
			fill[i] = files[i]
			missing = append(missing, i)
		}
	}
	if len(missing) > 0 {
		rs, err := newEncoder()
		if err != nil {
			return err
		}
		if err := rs.Reconstruct(valid, fill); err != nil {
			return fmt.Errorf("reconstruct shards: %w", err)
		}
		for _, i := range missing {
			if _, err := files[i].Seek(0, io.SeekStart); err != nil {
				return err
			}
			if err := verifyOpen(ctx, m, i, files[i]); err != nil {
				return err
			}
		}
	}
	readers := make([]io.Reader, model.DataShards)
	for i := range readers {
		if _, err := files[i].Seek(0, io.SeekStart); err != nil {
			return err
		}
		readers[i] = files[i]
	}
	output, err := os.CreateTemp(filepath.Dir(destination), ".p2pfs-output-")
	if err != nil {
		return err
	}
	tmp := output.Name()
	defer os.Remove(tmp)
	defer output.Close()
	h := sha256.New()
	n, err := io.CopyBuffer(io.MultiWriter(output, h), contextReader{ctx, io.LimitReader(io.MultiReader(readers...), m.Size)}, make([]byte, streamBlockSize))
	if err != nil {
		return err
	}
	if n != m.Size || hex.EncodeToString(h.Sum(nil)) != m.SHA256 {
		return fmt.Errorf("%w: reconstructed file digest or length mismatch", ErrShardCorrupt)
	}
	if err := output.Sync(); err != nil {
		return err
	}
	if err := output.Close(); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	// Hard-link publication is same-filesystem, atomic and fails if any path
	// already occupies destination. Rename would silently overwrite on Unix.
	if err := os.Link(tmp, destination); err != nil {
		if errors.Is(err, os.ErrExist) {
			return ErrDestinationExists
		}
		return fmt.Errorf("publish output atomically (filesystem must support hard links): %w", err)
	}
	return nil
}
