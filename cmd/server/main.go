// Command server runs the HTTPS metadata API and optional embedded UDP TURN server.
package main

import (
	"context"
	"crypto/tls"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/Paulkm2006/p2pfs/internal/coordinator"
)

func main() {
	if err := run(); err != nil {
		slog.Error("server stopped", "error", err)
		os.Exit(1)
	}
}
func run() error {
	listen := flag.String("listen", "127.0.0.1:8443", "HTTPS listen IP:port")
	db := flag.String("db", "p2pfs.db", "SQLite metadata path")
	cert := flag.String("tls-cert", "", "TLS certificate PEM path (required outside explicit local development)")
	key := flag.String("tls-key", "", "TLS private key PEM path")
	insecure := flag.Bool("insecure-loopback", false, "DEVELOPMENT ONLY: permit HTTP on a loopback IP listener")
	stun := flag.String("stun", "", "optional comma-separated stun: URLs")
	turnListen := flag.String("turn-listen", "", "UDP TURN listen IP:port; empty disables relay")
	turnIP := flag.String("turn-public-ip", "", "public IPv4 advertised for TURN allocations")
	turnAdvertise := flag.String("turn-advertise", "", "optional public TURN host:port (defaults to public IP and listen port)")
	turnLoopback := flag.Bool("turn-allow-loopback", false, "DEVELOPMENT ONLY: loopback TURN listener and allocations")
	relayMin := flag.Uint("turn-port-min", 49160, "minimum UDP relay allocation port")
	relayMax := flag.Uint("turn-port-max", 49287, "maximum UDP relay allocation port")
	flag.Parse()
	if err := validateHTTP(*listen, *cert, *key, *insecure); err != nil {
		return err
	}
	cfg := coordinator.Config{DatabasePath: *db}
	if *stun != "" {
		for _, u := range strings.Split(*stun, ",") {
			cfg.STUNURLs = append(cfg.STUNURLs, strings.TrimSpace(u))
		}
	}
	if *turnListen != "" {
		if *relayMin > 65534 || *relayMax > 65534 {
			return errors.New("TURN relay port must be at most 65534")
		}
		cfg.TURN = &coordinator.TURNConfig{ListenAddress: *turnListen, PublicIP: *turnIP, AdvertiseAddress: *turnAdvertise, AllowLoopback: *turnLoopback, RelayPortMin: uint16(*relayMin), RelayPortMax: uint16(*relayMax)}
	}
	s, err := coordinator.New(cfg)
	if err != nil {
		return err
	}
	defer s.Close()
	h := &http.Server{Addr: *listen, Handler: s.Handler(), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 15 * time.Second, WriteTimeout: 15 * time.Second, IdleTimeout: 60 * time.Second, MaxHeaderBytes: 16 << 10, TLSConfig: &tls.Config{MinVersion: tls.VersionTLS12}}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go func() {
		<-ctx.Done()
		shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = h.Shutdown(shutdown)
	}()
	slog.Info("p2pfs coordinator starting", "listen", *listen, "tls", !*insecure, "turn_udp", s.TURNAddress())
	slog.Warn("single collaboration space: restrict access to trusted participants; there is no account/admission system")
	if *insecure {
		err = h.ListenAndServe()
	} else {
		err = h.ListenAndServeTLS(*cert, *key)
	}
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}
func validateHTTP(address, cert, key string, insecure bool) error {
	if insecure {
		host, _, err := net.SplitHostPort(address)
		if err != nil {
			return err
		}
		ip := net.ParseIP(host)
		if ip == nil || !ip.IsLoopback() {
			return errors.New("insecure HTTP requires an explicit loopback IP listener")
		}
		if cert != "" || key != "" {
			return errors.New("choose either TLS or explicit loopback development HTTP")
		}
		return nil
	}
	if cert == "" || key == "" {
		return fmt.Errorf("TLS certificate and key required; local testing may use -insecure-loopback with 127.0.0.1 or ::1")
	}
	return nil
}
