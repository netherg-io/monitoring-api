package api

import (
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"

	"monitoring-api/internal/buffer"
	"monitoring-api/internal/collector"
	"monitoring-api/internal/docker"
	"monitoring-api/internal/storage/rediscache"
	"monitoring-api/internal/storage/tsdb"
)

type Config struct {
	Token            string
	Buffer           *buffer.Buffer
	Docker           *docker.Client
	Collector        *collector.Collector
	TSDB             *tsdb.Store
	Redis            *rediscache.Store
	ControlAllowlist []string
	ControlDenylist  []string
	CORSOrigins      []string
	Meter            *RequestsMeter
}

func NewRouter(cfg Config) http.Handler {
	r := chi.NewRouter()
	r.Use(middleware.RealIP)
	r.Use(middleware.Recoverer)
	r.Use(middleware.Timeout(20 * time.Second))
	r.Use(corsMiddleware(cfg.CORSOrigins))

	meter := cfg.Meter
	if meter == nil {
		meter = NewRequestsMeter(cfg.Redis, cfg.TSDB)
	}

	r.Get("/health", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, 200, map[string]string{"status": "ok"})
	})

	r.Route("/api", func(r chi.Router) {
		r.Use(authMiddleware(cfg.Token))
		r.Use(requestsMiddleware(meter))
		r.Get("/snapshot", handleSnapshot(cfg))
		r.Get("/history", handleHistory(cfg))
		r.Get("/containers", handleContainers(cfg))
		r.Get("/containers/{id}/logs", handleLogs(cfg))
		r.Post("/containers/{id}/action", handleAction(cfg))
		r.Get("/requests", handleRequests(meter))
		r.Get("/hosts", handleHosts())
	})

	return r
}

func authMiddleware(token string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			h := r.Header.Get("Authorization")
			if h == "" || !strings.HasPrefix(h, "Bearer ") || strings.TrimPrefix(h, "Bearer ") != token {
				writeJSON(w, 401, map[string]string{"error": "unauthorized"})
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}

func handleSnapshot(cfg Config) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, 200, cfg.Collector.Snapshot())
	}
}

func handleHistory(cfg Config) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		minutes, _ := strconv.Atoi(r.URL.Query().Get("minutes"))
		if minutes <= 0 {
			minutes = 15
		}
		since := time.Now().Add(-time.Duration(minutes) * time.Minute)
		points := cfg.Buffer.Since(since)
		// If the requested window exceeds what's in memory, supplement from TSDB.
		if cfg.TSDB != nil && (len(points) == 0 || points[0].TS.After(since.Add(time.Minute))) {
			if hist, err := cfg.TSDB.QueryRange(r.Context(), since); err == nil && len(hist) > 0 {
				if len(points) == 0 {
					points = hist
				} else {
					cutoff := points[0].TS
					merged := make([]buffer.Point, 0, len(hist)+len(points))
					for _, p := range hist {
						if p.TS.Before(cutoff) {
							merged = append(merged, p)
						}
					}
					merged = append(merged, points...)
					points = merged
				}
			}
		}
		writeJSON(w, 200, points)
	}
}

func handleContainers(cfg Config) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		list, err := cfg.Docker.List(r.Context())
		if err != nil {
			writeJSON(w, 500, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, 200, list)
	}
}

func handleLogs(cfg Config) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id := chi.URLParam(r, "id")
		tail, _ := strconv.Atoi(r.URL.Query().Get("tail"))
		if tail <= 0 {
			tail = 200
		}
		var since time.Time
		if s := r.URL.Query().Get("since"); s != "" {
			if ns, err := strconv.ParseInt(s, 10, 64); err == nil {
				since = time.Unix(0, ns)
			}
		}
		lines, err := cfg.Docker.Logs(r.Context(), id, tail, since)
		if err != nil {
			writeJSON(w, 500, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, 200, map[string]any{
			"lines":    lines,
			"fetched":  time.Now().UnixNano(),
		})
	}
}

type actionReq struct {
	Action string `json:"action"`
}

func handleAction(cfg Config) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id := chi.URLParam(r, "id")
		var req actionReq
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeJSON(w, 400, map[string]string{"error": "invalid body"})
			return
		}
		if req.Action != "start" && req.Action != "stop" && req.Action != "restart" {
			writeJSON(w, 400, map[string]string{"error": "invalid action"})
			return
		}

		name, err := cfg.Docker.Inspect(r.Context(), id)
		if err != nil {
			writeJSON(w, 404, map[string]string{"error": "container not found"})
			return
		}

		if !isControlAllowed(name, cfg.ControlAllowlist, cfg.ControlDenylist) {
			writeJSON(w, 403, map[string]string{"error": "control not permitted for " + name})
			return
		}

		if err := cfg.Docker.Action(r.Context(), id, req.Action); err != nil {
			writeJSON(w, 500, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, 200, map[string]string{"status": "ok", "action": req.Action, "name": name})
	}
}

func isControlAllowed(name string, allow, deny []string) bool {
	for _, d := range deny {
		if d == name {
			return false
		}
	}
	if len(allow) == 0 {
		return true
	}
	for _, a := range allow {
		if a == name {
			return true
		}
	}
	return false
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}
