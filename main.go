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
	"monitoring-api/internal/collector"
	"monitoring-api/internal/docker"
	"monitoring-api/internal/storage/rediscache"
	"monitoring-api/internal/storage/tsdb"
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
	corsOrigins := parseList(os.Getenv("CORS_ORIGINS"))

	hostID := envOr("HOST_ID", "")
	if hostID == "" {
		if h, err := os.Hostname(); err == nil {
			hostID = h
		} else {
			hostID = "default"
		}
	}

	dc, err := docker.New()
	if err != nil {
		log.Fatalf("docker client: %v", err)
	}
	defer dc.Close()

	rootCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tsdbStore, err := openTSDB(rootCtx, hostID)
	if err != nil {
		log.Printf("tsdb disabled: %v", err)
	}
	defer tsdbStore.Close()

	redisStore, err := openRedis(rootCtx, hostID)
	if err != nil {
		log.Printf("redis disabled: %v", err)
	}
	defer redisStore.Close()

	buf := buffer.New(points)
	cbuf := buffer.NewContainer(points)

	// Warm buffer from TSDB so /api/history is non-empty after restart.
	if tsdbStore != nil {
		since := time.Now().Add(-time.Duration(points) * interval)
		if hist, err := tsdbStore.QueryRange(rootCtx, since); err == nil {
			buf.Seed(hist)
			log.Printf("warmed buffer with %d points from tsdb", len(hist))
		} else {
			log.Printf("tsdb warmup: %v", err)
		}
	}

	// Async TSDB writer for live metrics.
	if tsdbStore != nil {
		writes := make(chan buffer.Point, 256)
		buf.SetOnPush(func(p buffer.Point) {
			select {
			case writes <- p:
			default:
				// drop on backpressure rather than blocking the collector
			}
		})
		go func() {
			for p := range writes {
				wctx, c := context.WithTimeout(rootCtx, 3*time.Second)
				if err := tsdbStore.WriteSnapshot(wctx, p); err != nil {
					log.Printf("tsdb write: %v", err)
				}
				c()
			}
		}()
	}

	col := collector.New(dc, buf, cbuf, interval)
	go col.Run(rootCtx)

	meter := api.NewRequestsMeter(redisStore, tsdbStore)
	go meter.FlushLoop(rootCtx)

	handler := api.NewRouter(api.Config{
		Token:            token,
		Buffer:           buf,
		ContainerBuffer:  cbuf,
		Docker:           dc,
		Collector:        col,
		TSDB:             tsdbStore,
		Redis:            redisStore,
		ControlAllowlist: allow,
		ControlDenylist:  deny,
		CORSOrigins:      corsOrigins,
		Meter:            meter,
	})

	srv := &http.Server{
		Addr:              addr,
		Handler:           handler,
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		log.Printf("monitoring-api listening on %s (interval=%s, buffer=%d, host_id=%s)", addr, interval, points, hostID)
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

func openTSDB(ctx context.Context, hostID string) (*tsdb.Store, error) {
	dsn := os.Getenv("TSDB_DSN")
	if dsn == "" {
		return nil, nil
	}
	octx, c := context.WithTimeout(ctx, 10*time.Second)
	defer c()
	return tsdb.Open(octx, dsn, hostID)
}

func openRedis(ctx context.Context, hostID string) (*rediscache.Store, error) {
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		return nil, nil
	}
	octx, c := context.WithTimeout(ctx, 5*time.Second)
	defer c()
	return rediscache.Open(octx, addr, hostID)
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
