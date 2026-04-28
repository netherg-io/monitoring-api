package api

import (
	"net/http"
	"strconv"
	"sync"
	"time"
)

// requestsMeter keeps a per-minute ring buffer of API request counts.
// Default window is 12h = 720 minute-buckets.
type requestsMeter struct {
	mu      sync.Mutex
	buckets []requestsBucket
	size    int // number of buckets (e.g. 720 for 12h)
}

type requestsBucket struct {
	Minute int64 `json:"ts"`    // unix seconds, truncated to minute
	Count  int64 `json:"count"` // requests served in that minute
}

func newRequestsMeter(hours int) *requestsMeter {
	if hours <= 0 {
		hours = 12
	}
	size := hours * 60
	return &requestsMeter{
		buckets: make([]requestsBucket, size),
		size:    size,
	}
}

// hit increments the counter for the bucket of the current minute.
func (m *requestsMeter) hit(now time.Time) {
	minute := now.Unix() - now.Unix()%60
	idx := int((minute / 60) % int64(m.size))

	m.mu.Lock()
	defer m.mu.Unlock()

	b := &m.buckets[idx]
	if b.Minute != minute {
		// reset stale bucket from a previous lap
		b.Minute = minute
		b.Count = 0
	}
	b.Count++
}

// snapshot returns the last `hours` of minute-buckets in chronological order.
// Buckets without traffic are returned with Count=0 to make charting trivial.
func (m *requestsMeter) snapshot(now time.Time, hours int) []requestsBucket {
	if hours <= 0 || hours*60 > m.size {
		hours = m.size / 60
	}
	count := hours * 60

	m.mu.Lock()
	defer m.mu.Unlock()

	endMinute := now.Unix() - now.Unix()%60
	out := make([]requestsBucket, count)
	for i := 0; i < count; i++ {
		minute := endMinute - int64((count-1-i)*60)
		idx := int((minute / 60) % int64(m.size))
		b := m.buckets[idx]
		if b.Minute != minute {
			out[i] = requestsBucket{Minute: minute, Count: 0}
		} else {
			out[i] = b
		}
	}
	return out
}

// requestsMiddleware bumps the meter for every request that flows through.
func requestsMiddleware(m *requestsMeter) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			m.hit(time.Now())
			next.ServeHTTP(w, r)
		})
	}
}

func handleRequests(m *requestsMeter) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		hours, _ := strconv.Atoi(r.URL.Query().Get("hours"))
		if hours <= 0 {
			hours = 12
		}
		writeJSON(w, 200, m.snapshot(time.Now(), hours))
	}
}
