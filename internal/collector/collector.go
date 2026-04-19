package collector

import (
	"context"
	"log"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/shirou/gopsutil/v4/cpu"
	"github.com/shirou/gopsutil/v4/disk"
	"github.com/shirou/gopsutil/v4/host"
	"github.com/shirou/gopsutil/v4/load"
	"github.com/shirou/gopsutil/v4/mem"
	"github.com/shirou/gopsutil/v4/net"

	"monitoring-api/internal/buffer"
	"monitoring-api/internal/docker"
)

type Collector struct {
	d        *docker.Client
	b        *buffer.Buffer
	interval time.Duration

	mu            sync.RWMutex
	lastSnapshot  Snapshot
	lastNetRx     uint64
	lastNetTx     uint64
	lastNetAt     time.Time
}

type HostInfo struct {
	Hostname       string  `json:"hostname"`
	OS             string  `json:"os"`
	Platform       string  `json:"platform"`
	PlatformVer    string  `json:"platform_version"`
	KernelVer      string  `json:"kernel_version"`
	UptimeSeconds  uint64  `json:"uptime_seconds"`
	CPUModel       string  `json:"cpu_model"`
	CPUCores       int     `json:"cpu_cores"`
}

type DiskInfo struct {
	Path       string  `json:"path"`
	Total      uint64  `json:"total"`
	Used       uint64  `json:"used"`
	UsedPct    float64 `json:"used_pct"`
}

type Snapshot struct {
	TS         time.Time         `json:"ts"`
	Host       HostInfo          `json:"host"`
	CPU        float64           `json:"cpu"`
	Mem        buffer.Point      `json:"mem_point"`
	Load       [3]float64        `json:"load"`
	NetRxBps   uint64            `json:"net_rx_bps"`
	NetTxBps   uint64            `json:"net_tx_bps"`
	Disks      []DiskInfo        `json:"disks"`
	Containers []ContainerRow    `json:"containers"`
}

type ContainerRow struct {
	docker.Info
	Stat *docker.Stat `json:"stat,omitempty"`
}

func New(d *docker.Client, b *buffer.Buffer, interval time.Duration) *Collector {
	return &Collector{d: d, b: b, interval: interval}
}

func (c *Collector) Run(ctx context.Context) {
	_, _ = cpu.Percent(0, false)
	if snap, err := c.collect(ctx); err == nil {
		c.store(snap)
	}

	ticker := time.NewTicker(c.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			snap, err := c.collect(ctx)
			if err != nil {
				log.Printf("collect error: %v", err)
				continue
			}
			c.store(snap)
		}
	}
}

func (c *Collector) Snapshot() Snapshot {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.lastSnapshot
}

func (c *Collector) store(s Snapshot) {
	c.mu.Lock()
	c.lastSnapshot = s
	c.mu.Unlock()
	c.b.Push(buffer.Point{
		TS:         s.TS,
		CPUPercent: s.CPU,
		MemPercent: s.Mem.MemPercent,
		MemUsed:    s.Mem.MemUsed,
		MemTotal:   s.Mem.MemTotal,
		NetRx:      s.NetRxBps,
		NetTx:      s.NetTxBps,
		Load1:      s.Load[0],
		Load5:      s.Load[1],
		Load15:     s.Load[2],
	})
}

func (c *Collector) collect(ctx context.Context) (Snapshot, error) {
	now := time.Now()
	snap := Snapshot{TS: now}

	if h, err := host.InfoWithContext(ctx); err == nil {
		snap.Host = HostInfo{
			Hostname: h.Hostname, OS: h.OS, Platform: h.Platform,
			PlatformVer: h.PlatformVersion, KernelVer: h.KernelVersion,
			UptimeSeconds: h.Uptime,
		}
	}
	if procPath := os.Getenv("HOST_PROC"); procPath != "" {
		if b, err := os.ReadFile(procPath + "/sys/kernel/hostname"); err == nil {
			snap.Host.Hostname = strings.TrimSpace(string(b))
		}
	}
	if cis, err := cpu.InfoWithContext(ctx); err == nil && len(cis) > 0 {
		snap.Host.CPUModel = cis[0].ModelName
	}
	if n, err := cpu.Counts(true); err == nil {
		snap.Host.CPUCores = n
	}

	if pcts, err := cpu.PercentWithContext(ctx, 0, false); err == nil && len(pcts) > 0 {
		snap.CPU = pcts[0]
	}

	if vm, err := mem.VirtualMemoryWithContext(ctx); err == nil {
		snap.Mem = buffer.Point{
			MemUsed: vm.Used, MemTotal: vm.Total, MemPercent: vm.UsedPercent,
		}
	}

	if la, err := load.AvgWithContext(ctx); err == nil {
		snap.Load = [3]float64{la.Load1, la.Load5, la.Load15}
	}

	rootfs := os.Getenv("HOST_ROOTFS")
	if rootfs == "" {
		if _, err := os.Stat("/host/rootfs"); err == nil {
			rootfs = "/host/rootfs"
		}
	}
	if parts, err := disk.PartitionsWithContext(ctx, false); err == nil {
		seen := map[string]bool{}
		for _, p := range parts {
			if seen[p.Mountpoint] || !isRealFS(p.Fstype) {
				continue
			}
			seen[p.Mountpoint] = true
			target := p.Mountpoint
			if rootfs != "" {
				target = strings.TrimRight(rootfs, "/") + p.Mountpoint
			}
			u, err := disk.UsageWithContext(ctx, target)
			if err != nil {
				continue
			}
			snap.Disks = append(snap.Disks, DiskInfo{
				Path: p.Mountpoint, Total: u.Total, Used: u.Used, UsedPct: u.UsedPercent,
			})
		}
	}

	if counters, err := net.IOCountersWithContext(ctx, false); err == nil && len(counters) > 0 {
		rx := counters[0].BytesRecv
		tx := counters[0].BytesSent
		if !c.lastNetAt.IsZero() {
			dt := now.Sub(c.lastNetAt).Seconds()
			if dt > 0 {
				if rx >= c.lastNetRx {
					snap.NetRxBps = uint64(float64(rx-c.lastNetRx) / dt)
				}
				if tx >= c.lastNetTx {
					snap.NetTxBps = uint64(float64(tx-c.lastNetTx) / dt)
				}
			}
		}
		c.lastNetRx, c.lastNetTx, c.lastNetAt = rx, tx, now
	}

	list, err := c.d.List(ctx)
	if err != nil {
		return snap, err
	}

	rows := make([]ContainerRow, len(list))
	var wg sync.WaitGroup
	for i, info := range list {
		rows[i] = ContainerRow{Info: info}
		if info.State != "running" {
			continue
		}
		wg.Add(1)
		go func(i int, id string) {
			defer wg.Done()
			sCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
			defer cancel()
			if st, err := c.d.Stats(sCtx, id); err == nil {
				rows[i].Stat = &st
			}
		}(i, info.ID)
	}
	wg.Wait()

	sort.Slice(rows, func(i, j int) bool {
		ci, cj := 0.0, 0.0
		if rows[i].Stat != nil {
			ci = rows[i].Stat.CPUPercent
		}
		if rows[j].Stat != nil {
			cj = rows[j].Stat.CPUPercent
		}
		if ci != cj {
			return ci > cj
		}
		return rows[i].Name < rows[j].Name
	})

	snap.Containers = rows
	return snap, nil
}

func isRealFS(t string) bool {
	switch t {
	case "", "tmpfs", "devtmpfs", "proc", "sysfs", "cgroup", "cgroup2",
		"devpts", "mqueue", "overlay", "overlay2", "squashfs", "autofs",
		"nsfs", "tracefs", "debugfs", "fusectl", "pstore", "bpf",
		"ramfs", "hugetlbfs", "configfs", "securityfs", "fuse.gvfsd-fuse",
		"binfmt_misc":
		return false
	}
	return true
}
