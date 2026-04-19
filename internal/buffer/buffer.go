package buffer

import (
	"sync"
	"time"
)

type Point struct {
	TS         time.Time `json:"ts"`
	CPUPercent float64   `json:"cpu"`
	MemPercent float64   `json:"mem"`
	MemUsed    uint64    `json:"mem_used"`
	MemTotal   uint64    `json:"mem_total"`
	NetRx      uint64    `json:"net_rx_bps"`
	NetTx      uint64    `json:"net_tx_bps"`
	Load1      float64   `json:"load1"`
	Load5      float64   `json:"load5"`
	Load15     float64   `json:"load15"`
}

type Buffer struct {
	mu     sync.RWMutex
	points []Point
	size   int
	head   int
	count  int
}

func New(size int) *Buffer {
	return &Buffer{points: make([]Point, size), size: size}
}

func (b *Buffer) Push(p Point) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.points[b.head] = p
	b.head = (b.head + 1) % b.size
	if b.count < b.size {
		b.count++
	}
}

func (b *Buffer) Latest() (Point, bool) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	if b.count == 0 {
		return Point{}, false
	}
	idx := (b.head - 1 + b.size) % b.size
	return b.points[idx], true
}

func (b *Buffer) Tail(n int) []Point {
	b.mu.RLock()
	defer b.mu.RUnlock()
	if n <= 0 || b.count == 0 {
		return nil
	}
	if n > b.count {
		n = b.count
	}
	out := make([]Point, n)
	start := (b.head - n + b.size) % b.size
	for i := 0; i < n; i++ {
		out[i] = b.points[(start+i)%b.size]
	}
	return out
}

func (b *Buffer) Since(since time.Time) []Point {
	b.mu.RLock()
	defer b.mu.RUnlock()
	if b.count == 0 {
		return nil
	}
	out := make([]Point, 0, b.count)
	for i := 0; i < b.count; i++ {
		idx := (b.head - b.count + i + b.size) % b.size
		p := b.points[idx]
		if !p.TS.Before(since) {
			out = append(out, p)
		}
	}
	return out
}
