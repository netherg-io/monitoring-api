package api

import (
	"context"
	"log"
	"net/http"
	"strconv"
	"time"

	"monitoring-api/internal/storage/rediscache"
	"monitoring-api/internal/storage/tsdb"
)

type RequestsMeter struct {
	redis *rediscache.Store
	tsdb  *tsdb.Store
}

type requestsBucket struct {
	Minute int64 `json:"ts"`
	Count  int64 `json:"count"`
}

func NewRequestsMeter(rs *rediscache.Store, ts *tsdb.Store) *RequestsMeter {
	return &RequestsMeter{redis: rs, tsdb: ts}
}

func (m *RequestsMeter) hit(ctx context.Context, now time.Time) {
	if m.redis == nil {
		return
	}
	if err := m.redis.IncrRequest(ctx, now); err != nil {
		log.Printf("requests incr: %v", err)
	}
}

// snapshot returns the last `hours` of minute buckets in chronological order.
// Missing buckets are zero-filled.
func (m *RequestsMeter) snapshot(ctx context.Context, now time.Time, hours int) []requestsBucket {
	if hours <= 0 {
		hours = 12
	}
	count := hours * 60
	endMinute := now.Unix() - now.Unix()%60
	startMinute := endMinute - int64((count-1)*60)

	out := make([]requestsBucket, count)
	for i := 0; i < count; i++ {
		out[i] = requestsBucket{Minute: startMinute + int64(i*60), Count: 0}
	}

	if m.redis != nil {
		from := time.Unix(startMinute, 0)
		to := time.Unix(endMinute, 0)
		if vals, err := m.redis.GetRequestRange(ctx, from, to); err == nil {
			for i := range out {
				if c, ok := vals[out[i].Minute]; ok {
					out[i].Count = c
				}
			}
			return out
		} else {
			log.Printf("requests redis range: %v", err)
		}
	}

	if m.tsdb != nil {
		if buckets, err := m.tsdb.QueryRequests(ctx, time.Unix(startMinute, 0)); err == nil {
			byMin := make(map[int64]int64, len(buckets))
			for _, b := range buckets {
				byMin[b.Minute.Unix()-b.Minute.Unix()%60] = b.Count
			}
			for i := range out {
				if c, ok := byMin[out[i].Minute]; ok {
					out[i].Count = c
				}
			}
		}
	}
	return out
}

// flushLoop periodically copies completed minute buckets from Redis to TSDB
// so request history is retained beyond Redis TTL / volume loss.
func (m *RequestsMeter) FlushLoop(ctx context.Context) {
	if m.redis == nil || m.tsdb == nil {
		return
	}
	ticker := time.NewTicker(60 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			now := time.Now()
			prev := time.Unix(now.Unix()-now.Unix()%60-60, 0)
			c, err := m.redis.GetRequestCount(ctx, prev)
			if err != nil {
				log.Printf("requests flush get: %v", err)
				continue
			}
			if c == 0 {
				continue
			}
			if err := m.tsdb.UpsertRequestCount(ctx, prev, c); err != nil {
				log.Printf("requests flush upsert: %v", err)
			}
		}
	}
}

func requestsMiddleware(m *RequestsMeter) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			m.hit(r.Context(), time.Now())
			next.ServeHTTP(w, r)
		})
	}
}

func handleRequests(m *RequestsMeter) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		hours, _ := strconv.Atoi(r.URL.Query().Get("hours"))
		if hours <= 0 {
			hours = 12
		}
		writeJSON(w, 200, m.snapshot(r.Context(), time.Now(), hours))
	}
}
