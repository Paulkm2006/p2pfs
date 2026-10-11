// Package codec stores and restores immutable, SHA-256 verified RS file shards.
package codec

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/Paulkm2006/p2pfs/internal/model"
)

const Format = 1
const Codec = "rs-vandermonde-gf256-k4m2-v1"

var (
	ErrInvalidManifest    = errors.New("invalid manifest")
	ErrShardCorrupt       = errors.New("invalid or corrupt shard")
	ErrInsufficientShards = errors.New("at least four valid, distinct shards are required")
	ErrSourceChanged      = errors.New("source file changed while encoding; retry publication")
	ErrDestinationExists  = errors.New("destination already exists; overwrite requires explicit approval")
)

// ManifestID hashes the canonical v1 manifest payload, excluding ID. Shards must
// be ordered by index; ValidateManifest rejects noncanonical order. JSON field
// order and escaping are fixed by this struct and encoding/json.
func ManifestID(m model.Manifest) string {
	payload := struct {
		Format       int           `json:"format"`
		Name         string        `json:"name"`
		Size         int64         `json:"size"`
		SHA256       string        `json:"sha256"`
		DataShards   int           `json:"data_shards"`
		ParityShards int           `json:"parity_shards"`
		Codec        string        `json:"codec"`
		Shards       []model.Shard `json:"shards"`
	}{m.Format, m.Name, m.Size, m.SHA256, m.DataShards, m.ParityShards, m.Codec, m.Shards}
	b, _ := json.Marshal(payload)
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

func validDigest(s string) bool {
	if len(s) != sha256.Size*2 || s != strings.ToLower(s) {
		return false
	}
	b, err := hex.DecodeString(s)
	return err == nil && len(b) == sha256.Size
}

func safeName(name string) bool {
	if name == "" || len(name) > 255 || !utf8.ValidString(name) || name == "." || name == ".." || strings.TrimRight(name, " .") != name {
		return false
	}
	for _, r := range name {
		if unicode.IsControl(r) || strings.ContainsRune("/\\:*?\"<>|", r) {
			return false
		}
	}
	stem := strings.ToUpper(strings.TrimRight(strings.SplitN(name, ".", 2)[0], " "))
	switch stem {
	case "CON", "PRN", "AUX", "NUL", "CONIN$", "CONOUT$":
		return false
	}
	if len(stem) == 4 && (strings.HasPrefix(stem, "COM") || strings.HasPrefix(stem, "LPT")) && stem[3] >= '0' && stem[3] <= '9' {
		return false
	}
	// Windows also treats superscript 1, 2 and 3 as reserved device numbers.
	for _, prefix := range []string{"COM", "LPT"} {
		for _, digit := range []string{"¹", "²", "³"} {
			if stem == prefix+digit {
				return false
			}
		}
	}
	return true
}

func shardSize(size int64) int64 {
	if size == 0 {
		return 1
	}
	return (size + model.DataShards - 1) / model.DataShards
}

// ValidateManifest enforces the complete supported encoding and immutable ID.
// Names are display-only and are never used as cache or destination paths.
func ValidateManifest(m model.Manifest) error {
	fail := func(reason string) error { return fmt.Errorf("%w: %s", ErrInvalidManifest, reason) }
	if m.Format != Format || m.Codec != Codec || m.DataShards != model.DataShards || m.ParityShards != model.ParityShards {
		return fail("unsupported format or encoding parameters")
	}
	if !safeName(m.Name) {
		return fail("unsafe display name")
	}
	if m.Size < 0 || m.Size > model.MaxFileSize {
		return fail("file size must be between 0 and 1 GiB")
	}
	if !validDigest(m.SHA256) || !validDigest(m.ID) {
		return fail("SHA-256 and ID must be lowercase hexadecimal digests")
	}
	if len(m.Shards) != model.TotalShards {
		return fail("exactly six shards are required")
	}
	for index, shard := range m.Shards {
		if shard.Index != index || shard.Size != shardSize(m.Size) || !validDigest(shard.SHA256) {
			return fail("invalid index, ordering, length, or shard digest")
		}
	}
	if m.Size == 0 {
		empty := sha256.Sum256(nil)
		if m.SHA256 != hex.EncodeToString(empty[:]) {
			return fail("incorrect empty-file digest")
		}
		zero := sha256.Sum256([]byte{0})
		for _, shard := range m.Shards {
			if shard.SHA256 != hex.EncodeToString(zero[:]) {
				return fail("empty-file shards must encode deterministic zero padding")
			}
		}
	}
	if ManifestID(m) != m.ID {
		return fail("manifest ID does not match its immutable payload")
	}
	return nil
}

// ShardPath returns an empty string for an invalid ID or index. Callers must
// validate manifests before using paths. No display name enters this path.
func ShardPath(cacheDir, fileID string, index int) string {
	if !validDigest(fileID) || index < 0 || index >= model.TotalShards {
		return ""
	}
	return filepath.Join(cacheDir, fileID, strconv.Itoa(index)+".shard")
}
