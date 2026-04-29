package buffer

import (
	"sync"
	"time"
)

type ContainerPoint struct {
	TS         time.Time `json:"ts"`
	CPU        float64   `json:"cpu"`
	MemPercent float64   `json:"mem"`
	MemUsed    uint64    `json:"mem_used"`
	MemLimit   uint64    `json:"mem_limit"`
	NetRxBps   uint64    `json:"net_rx_bps"`
	NetTxBps   uint64    `json:"net_tx_bps"`
}

type containerRing struct {
	points []ContainerPoint
	head   int
	count  int
}

func (r *containerRing) push(p ContainerPoint) {
	r.points[r.head] = p
	r.head = (r.head + 1) % len(r.points)
	if r.count < len(r.points) {
		r.count++
	}
}

func (r *containerRing) since(t time.Time) []ContainerPoint {
	if r.count == 0 {
		return nil
	}
	size := len(r.points)
	out := make([]ContainerPoint, 0, r.count)
	for i := 0; i < r.count; i++ {
		idx := (r.head - r.count + i + size) % size
		p := r.points[idx]
		if !p.TS.Before(t) {
			out = append(out, p)
		}
	}
	return out
}

type ContainerBuffer struct {
	mu    sync.RWMutex
	rings map[string]*containerRing
	size  int
}

func NewContainer(size int) *ContainerBuffer {
	if size < 10 {
		size = 10
	}
	return &ContainerBuffer{rings: make(map[string]*containerRing), size: size}
}

func (cb *ContainerBuffer) Push(id string, p ContainerPoint) {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	r, ok := cb.rings[id]
	if !ok {
		r = &containerRing{points: make([]ContainerPoint, cb.size)}
		cb.rings[id] = r
	}
	r.push(p)
}

func (cb *ContainerBuffer) Since(id string, t time.Time) []ContainerPoint {
	cb.mu.RLock()
	defer cb.mu.RUnlock()
	r, ok := cb.rings[id]
	if !ok {
		return nil
	}
	return r.since(t)
}

// Prune drops history for containers no longer present in active.
func (cb *ContainerBuffer) Prune(active map[string]bool) {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	for id := range cb.rings {
		if !active[id] {
			delete(cb.rings, id)
		}
	}
}
