package client

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"sync/atomic"
	"time"

	"github.com/Paulkm2006/p2pfs/internal/codec"
	"github.com/Paulkm2006/p2pfs/internal/model"
)

type choice struct {
	index  int
	peer   string
	rank   float64
	rarity int
}

// choose prioritizes rare missing indices, spreads work over peers, then considers
// measured throughput and recent errors. FixedScheduler supplies the comparison.
func (c *Client) choose(f model.FileInfo, have map[int]bool, limit int) []choice {
	byIndex := map[int][]string{}
	c.mu.Lock()
	self := c.state.Identity.UUID
	scores := make(map[string]peerScore, len(c.scores))
	for p, s := range c.scores {
		scores[p] = s
	}
	failedChoices := map[string]time.Time{}
	for key, at := range c.failedChoices {
		failedChoices[key] = at
	}
	c.mu.Unlock()
	for _, s := range f.Sources {
		if s.PeerID == self {
			continue
		}
		for _, i := range s.Indices {
			if i >= 0 && i < model.TotalShards && !have[i] {
				byIndex[i] = append(byIndex[i], s.PeerID)
			}
		}
	}
	var indices []int
	for i := range byIndex {
		indices = append(indices, i)
	}
	sort.Slice(indices, func(i, j int) bool {

		// An unavailable rare index must not starve healthy parity alternatives.
		allCooling := func(index int) bool {
			for _, p := range byIndex[index] {
				recentChoice := time.Since(failedChoices[choiceKey(f.Manifest.ID, index, p)]) < 10*time.Second
				if !recentChoice {
					return false
				}
			}
			return true
		}
		ci, cj := allCooling(indices[i]), allCooling(indices[j])
		if ci != cj {
			return !ci
		}
		if !c.cfg.FixedScheduler && len(byIndex[indices[i]]) != len(byIndex[indices[j]]) {
			return len(byIndex[indices[i]]) < len(byIndex[indices[j]])
		}
		return indices[i] < indices[j]
	})
	used := map[string]int{}
	var out []choice
	for _, i := range indices {
		peers := byIndex[i]
		sort.Strings(peers)
		best := ""
		bestScore := -1e30
		for _, p := range peers {
			s := scores[p]
			score := float64(-used[p]) * 1e12
			if time.Since(failedChoices[choiceKey(f.Manifest.ID, i, p)]) < 10*time.Second {
				score -= 1e14
			}
			if !c.cfg.FixedScheduler {
				score += s.rate
				if time.Since(s.failedAt) < 10*time.Second {
					score -= float64(s.failures) * 1e10
				}
			}
			if best == "" || score > bestScore {
				best = p
				bestScore = score
			}
		}
		if best != "" {
			used[best]++
			out = append(out, choice{index: i, peer: best, rank: bestScore, rarity: len(peers)})
			if len(out) >= limit {
				break
			}
		}
	}
	return out
}
func (c *Client) download(ctx context.Context, id string) {
	c.mu.Lock()
	t := *c.state.Tasks[id]
	c.mu.Unlock()
	for ctx.Err() == nil {
		info, e := c.file(ctx, t.FileID)
		if e != nil {
			c.taskState(id, "waiting", "无法发现来源: "+e.Error())
			if !wait(ctx, 2*time.Second) {
				return
			}
			continue
		}
		m := info.Manifest
		c.mu.Lock()
		if ctx.Err() != nil {
			c.mu.Unlock()
			return
		}
		c.state.Manifests[m.ID] = m
		c.state.Sharing[m.ID] = true
		e = c.saveLocked()
		c.mu.Unlock()
		if e != nil {
			c.taskState(id, "failed", e.Error())
			return
		}
		haveIndices := codec.Existing(m, c.cacheDir())
		// Additional space required excludes already verified cached bytes.
		cached := int64(0)
		for _, i := range haveIndices {
			cached += m.Shards[i].Size
		}
		cacheNeed := int64(model.TotalShards)*m.Shards[0].Size - cached
		required := uint64(max(cacheNeed, 0)+m.Size) + (16 << 20)
		for _, dir := range []string{c.cfg.DataDir, filepath.Dir(t.Destination)} {
			free, spaceErr := availableBytes(dir)
			if spaceErr != nil || free < required {
				if spaceErr == nil {
					spaceErr = fmt.Errorf("磁盘空间不足：还需预留约 %d 字节", required)
				}
				c.taskState(id, "failed", spaceErr.Error())
				return
			}
		}
		have := map[int]bool{}
		for _, i := range haveIndices {
			have[i] = true
		}
		c.mu.Lock()
		c.state.Tasks[id].CompletedShards = haveIndices
		c.saveLocked()
		c.mu.Unlock()
		if len(have) >= model.DataShards {
			c.taskState(id, "reconstructing", "")
			e = codec.Reconstruct(ctx, m, c.cacheDir(), t.Destination)
			if ctx.Err() != nil {
				return
			}
			if e != nil {
				c.taskState(id, "failed", "恢复文件失败: "+e.Error())
				return
			}
			c.taskState(id, "complete", "")
			slog.Info("download complete", "task", id, "file", m.ID, "bytes", m.Size)
			return
		}
		choices := c.choose(info, have, min(c.cfg.Concurrency, model.DataShards-len(have)))
		if len(choices) == 0 {
			c.taskState(id, "waiting", fmt.Sprintf("有效分片 %d/4，等待其他来源上线", len(have)))
			if !wait(ctx, 2*time.Second) {
				return
			}
			continue
		}
		c.taskState(id, "downloading", "")
		results := make(chan error, len(choices))
		for _, ch := range choices {
			go func(ch choice) {
				slog.Info("schedule shard", "task", id, "index", ch.index, "peer", ch.peer, "rarity", ch.rarity, "score", ch.rank)
				results <- c.fetchShard(ctx, m, ch.index, ch.peer, id)
			}(ch)
		}
		failed := false
		for range choices {
			if e = <-results; e != nil {
				failed = true
				if ctx.Err() == nil {
					c.taskState(id, "downloading", "正在换源: "+e.Error())
				}
			}
		}
		if ctx.Err() != nil {
			return
		}
		if failed && !wait(ctx, time.Second) {
			return
		}
	}
}

// limitedWriter rejects an overlong stream before it can fill the disk.
type limitedWriter struct {
	progress  *atomic.Int64
	w         io.Writer
	remaining int64
}

func (w *limitedWriter) Write(p []byte) (int, error) {
	if int64(len(p)) > w.remaining {
		return 0, errors.New("来源发送超过清单声明的长度")
	}
	n, e := w.w.Write(p)
	w.remaining -= int64(n)
	if n > 0 && w.progress != nil {
		w.progress.Store(time.Now().UnixNano())
	}
	return n, e
}
func (c *Client) fetchShard(ctx context.Context, m model.Manifest, index int, peer, taskID string) error {
	if index < 0 || index >= len(m.Shards) {
		return errors.New("分片索引无效")
	}
	target := codec.ShardPath(c.cacheDir(), m.ID, index)
	if _, err := c.cacheVersionDir(m, true); err != nil {
		return err
	}
	f, e := os.CreateTemp(filepath.Dir(target), ".receive-*")
	if e != nil {
		return e
	}
	temp := f.Name()
	defer os.Remove(temp)
	c.mu.Lock()
	tr := c.transport
	c.mu.Unlock()
	if tr == nil {
		f.Close()
		return errors.New("传输未连接")
	}
	started := time.Now()
	transferCtx, cancel := context.WithTimeout(ctx, 24*time.Hour)
	defer cancel()
	var progress atomic.Int64
	progress.Store(time.Now().UnixNano())
	watchDone := make(chan struct{})
	go func() {
		timer := time.NewTicker(time.Second)
		defer timer.Stop()
		for {
			select {
			case <-watchDone:
				return
			case <-transferCtx.Done():
				return
			case <-timer.C:
				if time.Since(time.Unix(0, progress.Load())) > 30*time.Second {
					cancel()
					return
				}
			}
		}
	}()
	stats, e := tr.Fetch(transferCtx, peer, m.ID, index, &limitedWriter{w: f, remaining: m.Shards[index].Size, progress: &progress})
	close(watchDone)
	if e == nil {
		e = f.Sync()
	}
	closeErr := f.Close()
	if e == nil {
		e = closeErr
	}
	if e == nil {
		e = codec.VerifyShard(m, index, temp)
	}
	if e == nil {
		c.transferMu.Lock()
		if ctx.Err() != nil {
			e = ctx.Err()
		} else if codec.VerifyShard(m, index, target) == nil {
			e = nil
		} else {
			e = os.Rename(temp, target)
		}
		c.transferMu.Unlock()
	}
	elapsed := time.Since(started)
	record := Transfer{PeerID: peer, Index: index, Path: stats.Path, Bytes: stats.Bytes, Milliseconds: elapsed.Milliseconds(), StartedAt: started}
	c.mu.Lock()
	s := c.scores[peer]
	if e != nil {
		s.failures++
		s.failedAt = time.Now()
		record.Error = e.Error()
	} else {
		s.failures = 0
		rate := float64(stats.Bytes) / max(elapsed.Seconds(), 0.001)
		if s.rate == 0 {
			s.rate = rate
		} else {
			s.rate = s.rate*.7 + rate*.3
		}
	}
	c.scores[peer] = s
	if e != nil {
		c.failedChoices[choiceKey(m.ID, index, peer)] = time.Now()
	} else {
		delete(c.failedChoices, choiceKey(m.ID, index, peer))
	}
	if taskID != "" {
		t := c.state.Tasks[taskID]
		t.Bytes += stats.Bytes
		t.Transfers = append(t.Transfers, record)
		if len(t.Transfers) > 200 {
			t.Transfers = t.Transfers[len(t.Transfers)-200:]
		}
		c.saveLocked()
	}
	c.mu.Unlock()
	slog.Info("shard transfer", "file", m.ID, "index", index, "peer", peer, "path", stats.Path, "bytes", stats.Bytes, "duration_ms", elapsed.Milliseconds(), "ok", e == nil)
	if e != nil {
		return e
	}
	return c.report(ctx, m)
}
func (c *Client) Retain(ctx context.Context, fileID string, indices []int) error {
	if e := c.beginOperation(fileID); e != nil {
		return e
	}
	defer c.endOperation(fileID)
	info, e := c.file(ctx, fileID)
	if e != nil {
		return e
	}
	if len(indices) == 0 {
		return errors.New("请选择要保留的分片")
	}
	seen := map[int]bool{}
	for _, i := range indices {
		if i < 0 || i >= model.TotalShards || seen[i] {
			return errors.New("分片索引无效或重复")
		}
		seen[i] = true
	}

	needed := int64(0)
	for _, i := range indices {
		if codec.VerifyShard(info.Manifest, i, codec.ShardPath(c.cacheDir(), fileID, i)) != nil {
			needed += info.Manifest.Shards[i].Size
		}
	}
	free, spaceErr := availableBytes(c.cfg.DataDir)
	if spaceErr != nil {
		return spaceErr
	}
	if free < uint64(needed)+(16<<20) {
		return errors.New("保留分片所需的磁盘空间不足")
	}
	c.mu.Lock()
	c.state.Manifests[fileID] = info.Manifest
	c.state.Sharing[fileID] = true
	e = c.saveLocked()
	c.mu.Unlock()
	if e != nil {
		return e
	}
	for _, i := range indices {
		if codec.VerifyShard(info.Manifest, i, codec.ShardPath(c.cacheDir(), fileID, i)) == nil {
			continue
		}
		have := map[int]bool{}
		for j := 0; j < model.TotalShards; j++ {
			if j != i {
				have[j] = true
			}
		}
		choices := c.choose(info, have, 1)
		if len(choices) == 0 {
			return fmt.Errorf("分片 %d 无可用来源", i)
		}
		var failures []error
		for _, source := range info.Sources {
			if source.PeerID == c.Identity().UUID {
				continue
			}
			holds := false
			for _, j := range source.Indices {
				if j == i {
					holds = true
				}
			}
			if !holds {
				continue
			}
			e = c.fetchShard(ctx, info.Manifest, i, source.PeerID, "")
			if e == nil {
				break
			}
			failures = append(failures, e)
		}
		if e != nil {
			return errors.Join(failures...)
		}
	}
	return c.report(ctx, info.Manifest)
}
func (c *Client) Unshare(ctx context.Context, fileID string, deleteCache bool) error {
	if e := c.beginOperation(fileID); e != nil {
		return e
	}
	defer c.endOperation(fileID)
	c.mu.Lock()
	m, ok := c.state.Manifests[fileID]
	if c.publishing > 0 {
		c.mu.Unlock()
		return errors.New("正在发布文件，请等待发布结束再停止共享")
	}
	if !ok {
		c.mu.Unlock()
		return errors.New("本机没有该文件")
	}
	for _, t := range c.state.Tasks {
		if t.FileID == fileID && (c.runs[t.ID] != nil || (t.State != "complete" && t.State != "cancelled" && t.State != "failed")) {
			c.mu.Unlock()
			return errors.New("该文件仍用于未完成任务，请先取消任务")
		}
	}
	c.state.Sharing[fileID] = false
	tr := c.transport
	e := c.saveLocked()
	c.mu.Unlock()
	if e != nil {
		return e
	}
	if tr != nil {
		tr.StopSharing(fileID)
	}
	if e = c.report(ctx, m); e != nil {
		return e
	}
	if deleteCache {
		if _, err := c.cacheVersionDir(m, false); err != nil {
			return err
		}
		c.transferMu.Lock()
		defer c.transferMu.Unlock()
		for i := 0; i < model.TotalShards; i++ {
			if e = os.Remove(codec.ShardPath(c.cacheDir(), fileID, i)); e != nil && !os.IsNotExist(e) {
				return e
			}
		}
	}
	return nil
}

type LocalFile struct {
	Manifest model.Manifest `json:"manifest"`
	Sharing  bool           `json:"sharing"`
	Indices  []int          `json:"indices"`
	Bytes    int64          `json:"bytes"`
}

func (c *Client) LocalFiles() []LocalFile {
	c.mu.Lock()
	var files []LocalFile
	for id, m := range c.state.Manifests {
		files = append(files, LocalFile{Manifest: m, Sharing: c.state.Sharing[id]})
	}
	c.mu.Unlock()
	for i := range files {
		f := &files[i]
		f.Indices = codec.Existing(f.Manifest, c.cacheDir())
		for _, j := range f.Indices {
			f.Bytes += f.Manifest.Shards[j].Size
		}
	}
	return files
}

// cacheVersionDir rejects symlinked cache/version directories before any write or
// delete. Cache directories are private local state, never peer-supplied paths.
func (c *Client) cacheVersionDir(m model.Manifest, create bool) (string, error) {
	if err := codec.ValidateManifest(m); err != nil {
		return "", err
	}
	base := c.cacheDir()
	dir := filepath.Dir(codec.ShardPath(base, m.ID, 0))
	for _, path := range []string{base, dir} {
		if create {
			if err := os.Mkdir(path, 0700); err != nil && !os.IsExist(err) {
				return "", err
			}
		}
		st, err := os.Lstat(path)
		if err != nil {
			return "", err
		}
		if !st.IsDir() || st.Mode()&os.ModeSymlink != 0 {
			return "", errors.New("缓存目录不能是符号链接或普通文件")
		}
	}
	return dir, nil
}

func choiceKey(fileID string, index int, peer string) string {
	return fmt.Sprintf("%s/%d/%s", fileID, index, peer)
}
