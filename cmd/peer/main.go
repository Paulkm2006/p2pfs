// peer is the headless client for reproducible tests and source nodes.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/Paulkm2006/p2pfs/internal/client"
)

func main() {
	if err := run(); err != nil {
		slog.Error("peer stopped", "error", err)
		os.Exit(1)
	}
}
func run() error {
	var cfg client.Config
	var publish, download, output, retain, indices, unshare string
	var list, remove bool
	flag.StringVar(&cfg.ServerURL, "server", "http://127.0.0.1:8080", "HTTPS coordinator URL")
	flag.StringVar(&cfg.DataDir, "data", "./peer-data", "private local data directory, unique per peer")
	flag.StringVar(&cfg.Name, "name", "peer", "display name")
	flag.BoolVar(&cfg.InsecureLocal, "insecure-local", false, "allow HTTP only for explicit loopback testing")
	flag.BoolVar(&cfg.ForceRelay, "relay", false, "force server UDP relay for diagnostic runs")
	flag.IntVar(&cfg.Concurrency, "concurrency", 3, "maximum simultaneous shard requests (1..6)")
	flag.Int64Var(&cfg.UploadBytesPerSecond, "upload-limit", 0, "aggregate upload payload limit in bytes/second (0 unlimited)")
	flag.BoolVar(&cfg.FixedScheduler, "fixed", false, "fixed scheduler comparison mode")
	flag.StringVar(&publish, "publish", "", "publish this file and continue serving")
	flag.StringVar(&download, "download", "", "download manifest ID then exit")
	flag.StringVar(&output, "out", "", "new output path for --download")
	flag.StringVar(&retain, "retain", "", "retain selected shards for manifest ID then keep serving")
	flag.StringVar(&indices, "indices", "0,1", "comma-separated shard indices for --retain")
	flag.StringVar(&unshare, "unshare", "", "stop sharing this manifest ID")
	flag.BoolVar(&remove, "delete-cache", false, "with --unshare remove only this file's cached shards")
	flag.BoolVar(&list, "list", false, "list known files then exit")
	flag.Parse()
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stderr, nil)))
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	c, e := client.New(cfg)
	if e != nil {
		return e
	}
	defer c.Close()
	if e = c.Start(ctx); e != nil {
		return e
	}
	slog.Info("peer online", "uuid", c.Identity().UUID)
	if list {
		files, e := c.ListFiles(ctx)
		if e != nil {
			return e
		}
		return json.NewEncoder(os.Stdout).Encode(files)
	}
	if publish != "" {
		m, e := c.Publish(ctx, publish)
		if e != nil {
			return e
		}
		if e = json.NewEncoder(os.Stdout).Encode(m); e != nil {
			return e
		}
	}
	if retain != "" {
		var parsed []int
		for _, x := range strings.Split(indices, ",") {
			i, e := strconv.Atoi(strings.TrimSpace(x))
			if e != nil {
				return e
			}
			parsed = append(parsed, i)
		}
		if e = c.Retain(ctx, retain, parsed); e != nil {
			return e
		}
		slog.Info("shards retained", "file", retain, "indices", parsed)
	}
	if unshare != "" {
		return c.Unshare(ctx, unshare, remove)
	}
	if download != "" {
		if output == "" {
			return fmt.Errorf("--download requires --out")
		}
		id, e := c.StartDownload(download, output)
		if e != nil {
			return e
		}
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-ticker.C:
				for _, t := range c.Tasks() {
					if t.ID != id {
						continue
					}
					if t.State == "complete" {
						return json.NewEncoder(os.Stdout).Encode(t)
					}
					if t.State == "failed" {
						return fmt.Errorf("%s", t.Error)
					}
					slog.Info("download status", "state", t.State, "verified", len(t.CompletedShards), "error", t.Error)
				}
			}
		}
	}
	<-ctx.Done()
	return nil
}
