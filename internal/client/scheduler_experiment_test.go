package client

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"testing"
	"time"

	"github.com/Paulkm2006/p2pfs/internal/codec"
	"github.com/Paulkm2006/p2pfs/internal/model"
)

const schedulerExperimentBytes = int64(100 << 20)
const schedulerExperimentRounds = 5

type schedulerMeasurement struct {
	Scenario     string     `json:"scenario"`
	Round        int        `json:"round"`
	Position     int        `json:"position"`
	Policy       string     `json:"policy"`
	Seconds      float64    `json:"seconds"`
	Complete     bool       `json:"complete"`
	PayloadBytes int64      `json:"payload_bytes"`
	Transfers    []Transfer `json:"transfers"`
	Error        string     `json:"error,omitempty"`
}

// This opt-in comparison sends the whole payload over real UDP. The source's
// sparse allocation saves fixture-generation work, not network traffic. The
// fixture and thresholds are predeclared in docs/scheduler-results.md.
func TestSchedulerControlledComparison(t *testing.T) {
	if os.Getenv("P2PFS_SCHEDULER_EXPERIMENT") != "1" {
		t.Skip("set P2PFS_SCHEDULER_EXPERIMENT=1 for the 20-transfer controlled comparison")
	}
	t.Logf("environment go=%s os=%s arch=%s logical_cpus=%d gomaxprocs=%d start_utc=%s", runtime.Version(), runtime.GOOS, runtime.GOARCH, runtime.NumCPU(), runtime.GOMAXPROCS(0), time.Now().UTC().Format(time.RFC3339))
	path := schedulerSource(t)
	for _, scenario := range []string{"heterogeneous", "uniform"} {
		t.Run(scenario, func(t *testing.T) {
			url, _ := testServer(t)
			sources := []*Client{testPeer(t, url), testPeer(t, url), testPeer(t, url)}
			sort.Slice(sources, func(i, j int) bool { return sources[i].Identity().UUID < sources[j].Identity().UUID })
			var manifest model.Manifest
			for i, initial := range sources {
				cfg, uuid := initial.cfg, initial.Identity().UUID
				if err := initial.Close(); err != nil {
					t.Fatal(err)
				}
				cfg.Concurrency = 3
				if scenario == "heterogeneous" && i == 0 {
					cfg.UploadBytesPerSecond = 8 << 20
				}
				// Recreate before publication so register constructs the transport
				// with the actual upload rate, rather than only editing cfg.
				source, err := New(cfg)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { source.Close() })
				if err = source.Start(context.Background()); err != nil {
					t.Fatal(err)
				}
				if source.Identity().UUID != uuid {
					t.Fatal("source UUID changed after restart")
				}
				m, err := source.Publish(context.Background(), path)
				if err != nil {
					t.Fatal(err)
				}
				if i > 0 && m.ID != manifest.ID {
					t.Fatal("source manifests differ")
				}
				manifest = m
				sources[i] = source
				t.Logf("source rank=%d uuid=%s upload_bytes_per_second=%d indices=[0 1 2 3 4 5]", i, uuid, cfg.UploadBytesPerSecond)
			}
			initial := testPeer(t, url)
			cfg, uuid := initial.cfg, initial.Identity().UUID
			cfg.Concurrency = 3
			if err := initial.Close(); err != nil {
				t.Fatal(err)
			}
			pristine, err := os.ReadFile(filepath.Join(cfg.DataDir, "config.json"))
			if err != nil {
				t.Fatal(err)
			}
			t.Logf("fixture file_id=%s sha256=%s source_bytes=%d downloader_uuid=%s concurrency=3 rounds=%d", manifest.ID, manifest.SHA256, manifest.Size, uuid, schedulerExperimentRounds)
			measurements := make([]schedulerMeasurement, 0, schedulerExperimentRounds*2)
			for round := 1; round <= schedulerExperimentRounds; round++ {
				order := []bool{true, false}
				if round%2 == 0 {
					order = []bool{false, true}
				}
				for position, fixed := range order {
					cfg.FixedScheduler = fixed
					policy := "adaptive"
					if fixed {
						policy = "fixed"
					}
					r := schedulerMeasurement{Scenario: scenario, Round: round, Position: position + 1, Policy: policy}
					if err := schedulerTrial(t, cfg, uuid, pristine, manifest, &r); err != nil {
						r.Error = err.Error()
						t.Errorf("%s round %d: %v", policy, round, err)
					}
					raw, _ := json.Marshal(r)
					t.Logf("SCHEDULER_RESULT %s", raw)
					measurements = append(measurements, r)
					// Equal settle interval outside both policies' measurement windows.
					time.Sleep(200 * time.Millisecond)
				}
			}
			schedulerSummary(t, scenario, measurements)
		})
	}
}

func schedulerSource(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "scheduler-100mib.bin")
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if err = f.Truncate(schedulerExperimentBytes); err != nil {
		t.Fatal(err)
	}
	marker := []byte("p2pfs-scheduler-v1!")
	for _, offset := range []int64{0, 1<<20 - 1, 25<<20 - 1, 25 << 20, 50<<20 - 1, 50 << 20, 75<<20 - 1, 75 << 20, schedulerExperimentBytes - int64(len(marker))} {
		if _, err = f.WriteAt(marker, offset); err != nil {
			t.Fatal(err)
		}
	}
	if err = f.Sync(); err != nil {
		t.Fatal(err)
	}
	return path
}

func schedulerTrial(t *testing.T, cfg Config, uuid string, pristine []byte, m model.Manifest, result *schedulerMeasurement) (err error) {
	t.Helper()
	// Only this test's private temporary state is reset. Preserve the same
	// identity but remove cached shards, learned scores and task history.
	if err = os.RemoveAll(filepath.Join(cfg.DataDir, "cache")); err != nil {
		return err
	}
	if err = os.WriteFile(filepath.Join(cfg.DataDir, "config.json"), pristine, 0600); err != nil {
		return err
	}
	c, err := New(cfg)
	if err != nil {
		return err
	}
	defer c.Close()
	if err = c.Start(context.Background()); err != nil {
		return err
	}
	if c.Identity().UUID != uuid || len(codec.Existing(m, c.cacheDir())) != 0 || len(c.scores) != 0 {
		return fmt.Errorf("downloader identity, cache or score isolation failed")
	}
	info, err := c.file(context.Background(), m.ID)
	if err != nil {
		return err
	}
	if len(info.Sources) != 3 {
		return fmt.Errorf("want exactly three sources, got %d", len(info.Sources))
	}
	for _, source := range info.Sources {
		if len(source.Indices) != model.TotalShards {
			return fmt.Errorf("source %s does not hold all six indices", source.PeerID)
		}
	}
	destination := filepath.Join(t.TempDir(), "reconstructed.bin")
	started := time.Now()
	id, err := c.StartDownload(m.ID, destination)
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			_ = c.Cancel(id)
		}
	}()
	deadline := time.Now().Add(90 * time.Second)
	var completed Task
	for time.Now().Before(deadline) {
		for _, task := range c.Tasks() {
			if task.ID != id {
				continue
			}
			if task.State == "failed" {
				return fmt.Errorf("download failed: %s", task.Error)
			}
			if task.State == "complete" {
				completed = task
				break
			}
		}
		if completed.ID != "" {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	result.Seconds = time.Since(started).Seconds()
	if completed.ID == "" {
		return fmt.Errorf("download exceeded 90 seconds")
	}
	result.PayloadBytes, result.Transfers = completed.Bytes, completed.Transfers
	if completed.Bytes != schedulerExperimentBytes || len(completed.Transfers) != model.DataShards {
		return fmt.Errorf("unexpected payload or transfer count: %d bytes, %d transfers", completed.Bytes, len(completed.Transfers))
	}
	for _, transfer := range completed.Transfers {
		if transfer.Error != "" || transfer.Path != "direct" || transfer.Bytes != schedulerExperimentBytes/model.DataShards {
			return fmt.Errorf("unexpected transfer: %+v", transfer)
		}
	}
	output, err := os.Open(destination)
	if err != nil {
		return err
	}
	hash := sha256.New()
	n, hashErr := io.CopyBuffer(hash, output, make([]byte, 256<<10))
	closeErr := output.Close()
	if hashErr != nil || closeErr != nil || n != m.Size || fmt.Sprintf("%x", hash.Sum(nil)) != m.SHA256 {
		return fmt.Errorf("independent output integrity check failed: bytes=%d read=%v close=%v", n, hashErr, closeErr)
	}
	// The complete state precedes the worker's final cleanup by a few
	// instructions. Wait for it before withdrawing the downloader's holdings.
	c.mu.Lock()
	run := c.runs[id]
	c.mu.Unlock()
	if run != nil {
		<-run.done
	}
	if err = c.Unshare(context.Background(), m.ID, true); err != nil {
		return err
	}
	result.Complete = true
	return nil
}

func schedulerSummary(t *testing.T, scenario string, results []schedulerMeasurement) {
	t.Helper()
	medians := map[string]float64{}
	for _, policy := range []string{"fixed", "adaptive"} {
		var values []float64
		var payload int64
		for _, result := range results {
			if result.Policy == policy && result.Complete {
				values = append(values, result.Seconds)
				payload += result.PayloadBytes
			}
		}
		if len(values) == 0 {
			t.Logf("SCHEDULER_SUMMARY scenario=%s policy=%s completed=0/%d", scenario, policy, schedulerExperimentRounds)
			continue
		}
		sort.Float64s(values)
		median := schedulerMedian(values)
		deviations := make([]float64, len(values))
		for i, value := range values {
			deviations[i] = math.Abs(value - median)
		}
		sort.Float64s(deviations)
		medians[policy] = median
		t.Logf("SCHEDULER_SUMMARY scenario=%s policy=%s completed=%d/%d median_seconds=%.9f min_seconds=%.9f max_seconds=%.9f mad_seconds=%.9f payload_bytes=%d", scenario, policy, len(values), schedulerExperimentRounds, median, values[0], values[len(values)-1], schedulerMedian(deviations), payload)
	}
	if medians["fixed"] > 0 && medians["adaptive"] > 0 {
		improvement := 100 * (1 - medians["adaptive"]/medians["fixed"])
		goalMet := improvement >= 10
		if scenario == "uniform" {
			goalMet = improvement >= -10
		}
		t.Logf("SCHEDULER_GOAL scenario=%s median_improvement_percent=%.6f goal_met=%t all_runs_complete=%t", scenario, improvement, goalMet, schedulerAllComplete(results))
	}
}

func schedulerMedian(sorted []float64) float64 {
	n := len(sorted)
	if n%2 == 0 {
		return (sorted[n/2-1] + sorted[n/2]) / 2
	}
	return sorted[n/2]
}

func schedulerAllComplete(results []schedulerMeasurement) bool {
	for _, result := range results {
		if !result.Complete {
			return false
		}
	}
	return len(results) == schedulerExperimentRounds*2
}
