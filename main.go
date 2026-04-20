package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"monitoring-api/internal/api"
	"monitoring-api/internal/buffer"
	"monitoring-api/internal/cloudflare"
	"monitoring-api/internal/collector"
	"monitoring-api/internal/docker"
)

func main() {
	addr := envOr("LISTEN_ADDR", ":8080")
	token := os.Getenv("API_TOKEN")
	if token == "" {
		log.Fatal("API_TOKEN is required")
	}
	interval := parseDuration(envOr("COLLECT_INTERVAL", "2s"))
	points := parseInt(envOr("BUFFER_POINTS", "1800"))
	allow := parseList(os.Getenv("CONTROL_ALLOWLIST"))
	deny := parseList(os.Getenv("CONTROL_DENYLIST"))

	dc, err := docker.New()
	if err != nil {
		log.Fatalf("docker client: %v", err)
	}
	defer dc.Close()

	buf := buffer.New(points)
	col := collector.New(dc, buf, interval)

	cf := cloudflare.New(
		os.Getenv("CF_API_TOKEN"),
		os.Getenv("CF_ACCOUNT_ID"),
		os.Getenv("CF_TUNNEL_ID"),
	)
	cfInterval := parseDuration(envOr("CF_REFRESH_INTERVAL", "60s"))

	rootCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go col.Run(rootCtx)
	if cf.Enabled() {
		go cf.Run(rootCtx, cfInterval)
		log.Printf("cloudflare tunnel sync enabled (interval=%s)", cfInterval)
	}

	handler := api.NewRouter(api.Config{
		Token:            token,
		Buffer:           buf,
		Docker:           dc,
		Collector:        col,
		Cloudflare:       cf,
		ControlAllowlist: allow,
		ControlDenylist:  deny,
	})

	srv := &http.Server{
		Addr:              addr,
		Handler:           handler,
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		log.Printf("monitoring-api listening on %s (interval=%s, buffer=%d)", addr, interval, points)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("serve: %v", err)
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop

	shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelShutdown()
	_ = srv.Shutdown(shutdownCtx)
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func parseDuration(s string) time.Duration {
	d, err := time.ParseDuration(s)
	if err != nil || d < 500*time.Millisecond {
		return 2 * time.Second
	}
	return d
}

func parseInt(s string) int {
	n, err := strconv.Atoi(s)
	if err != nil || n < 10 {
		return 1800
	}
	return n
}

func parseList(s string) []string {
	if s == "" {
		return nil
	}
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}
