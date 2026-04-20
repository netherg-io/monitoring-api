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

type containerNetEntry struct {
	rx, tx uint64
	at     time.Time
}

type Collector struct {
	d        *docker.Client
	b        *buffer.Buffer
	interval time.Duration

	mu               sync.RWMutex
	lastSnapshot     Snapshot
	lastNetRx        uint64
	lastNetTx        uint64
	lastNetAt        time.Time
	lastContainerNet map[string]containerNetEntry
	lastDiskRead     uint64
	lastDiskWrite    uint64
	lastDiskAt       time.Time
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
	Path    string  `json:"path"`
	Device  string  `json:"device"`
	Total   uint64  `json:"total"`
	Used    uint64  `json:"used"`
	UsedPct float64 `json:"used_pct"`
}

type Snapshot struct {
	TS         time.Time      `json:"ts"`
	Host       HostInfo       `json:"host"`
	CPU        float64        `json:"cpu"`
	CPUPerCore []float64      `json:"cpu_per_core"`
	Mem        buffer.Point   `json:"mem_point"`
	Load       [3]float64     `json:"load"`
	NetRxBps   uint64         `json:"net_rx_bps"`
	NetTxBps   uint64         `json:"net_tx_bps"`
	DiskReadBps  uint64       `json:"disk_read_bps"`
	DiskWriteBps uint64       `json:"disk_write_bps"`
	Disks      []DiskInfo     `json:"disks"`
	Containers []ContainerRow `json:"containers"`
}

type ContainerRow struct {
	docker.Info
	Stat *docker.Stat `json:"stat,omitempty"`
}

func New(d *docker.Client, b *buffer.Buffer, interval time.Duration) *Collector {
	return &Collector{
		d:                d,
		b:                b,
		interval:         interval,
		lastContainerNet: make(map[string]containerNetEntry),
	}
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
		CPUPerCore: append([]float64(nil), s.CPUPerCore...),
		MemPercent: s.Mem.MemPercent,
		MemUsed:    s.Mem.MemUsed,
		MemTotal:   s.Mem.MemTotal,
		NetRx:      s.NetRxBps,
		NetTx:      s.NetTxBps,
		DiskReadBps:  s.DiskReadBps,
		DiskWriteBps: s.DiskWriteBps,
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

	if pcts, err := cpu.PercentWithContext(ctx, 0, true); err == nil && len(pcts) > 0 {
		snap.CPUPerCore = pcts
		var sum float64
		for _, p := range pcts {
			sum += p
		}
		snap.CPU = sum / float64(len(pcts))
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
	var diskFilter []string
	for _, d := range strings.Split(os.Getenv("MONITOR_DISKS"), ",") {
		if d = strings.TrimSpace(d); d != "" {
			diskFilter = append(diskFilter, d)
		}
	}
	if parts, err := disk.PartitionsWithContext(ctx, false); err == nil {
		seen := map[string]bool{}
		// When diskFilter is set, pick only the largest partition per physical device,
		// so a disk with multiple partitions (e.g. sda1=/boot/efi, sda2=/) is shown once.
		perDevice := map[string]DiskInfo{}
		for _, p := range parts {
			if seen[p.Mountpoint] || !isRealFS(p.Fstype) {
				continue
			}
			if len(diskFilter) > 0 && !diskMatchesFilter(p.Device, diskFilter) {
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
			info := DiskInfo{
				Path: p.Mountpoint, Device: p.Device, Total: u.Total, Used: u.Used, UsedPct: u.UsedPercent,
			}
			if len(diskFilter) > 0 {
				root := physicalDevice(p.Device)
				if cur, ok := perDevice[root]; !ok || info.Total > cur.Total {
					perDevice[root] = info
				}
				continue
			}
			snap.Disks = append(snap.Disks, info)
		}
		if len(diskFilter) > 0 {
			roots := make([]string, 0, len(perDevice))
			for r := range perDevice {
				roots = append(roots, r)
			}
			sort.Strings(roots)
			for _, r := range roots {
				snap.Disks = append(snap.Disks, perDevice[r])
			}
		}
	}

	rx, tx, netOK := hostNetCounters()
	if !netOK {
		if counters, err := net.IOCountersWithContext(ctx, false); err == nil && len(counters) > 0 {
			rx = counters[0].BytesRecv
			tx = counters[0].BytesSent
			netOK = true
		}
	}
	if netOK {
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

	if iocs, err := disk.IOCountersWithContext(ctx); err == nil && len(iocs) > 0 {
		var dr, dw uint64
		for name, st := range iocs {
			if !isRealBlockDevice(name) {
				continue
			}
			if len(diskFilter) > 0 && !diskMatchesFilter(name, diskFilter) {
				continue
			}
			dr += st.ReadBytes
			dw += st.WriteBytes
		}
		if !c.lastDiskAt.IsZero() {
			dt := now.Sub(c.lastDiskAt).Seconds()
			if dt > 0 {
				if dr >= c.lastDiskRead {
					snap.DiskReadBps = uint64(float64(dr-c.lastDiskRead) / dt)
				}
				if dw >= c.lastDiskWrite {
					snap.DiskWriteBps = uint64(float64(dw-c.lastDiskWrite) / dt)
				}
			}
		}
		c.lastDiskRead, c.lastDiskWrite, c.lastDiskAt = dr, dw, now
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

	newLastNet := make(map[string]containerNetEntry, len(rows))
	for i := range rows {
		if rows[i].Stat == nil {
			continue
		}
		rx := rows[i].Stat.NetRx
		tx := rows[i].Stat.NetTx
		id := rows[i].ID
		if prev, ok := c.lastContainerNet[id]; ok {
			dt := now.Sub(prev.at).Seconds()
			if dt > 0 {
				if rx >= prev.rx {
					rows[i].Stat.NetRxBps = uint64(float64(rx-prev.rx) / dt)
				}
				if tx >= prev.tx {
					rows[i].Stat.NetTxBps = uint64(float64(tx-prev.tx) / dt)
				}
			}
		}
		newLastNet[id] = containerNetEntry{rx: rx, tx: tx, at: now}
	}
	c.lastContainerNet = newLastNet

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

// isRealBlockDevice excludes partitions and virtual/loop/ram devices so disk
// I/O totals only reflect physical drives. Matches gopsutil key format (e.g.
// "sda", "sda1", "nvme0n1", "nvme0n1p1", "dm-0", "loop3", "ram0").
func isRealBlockDevice(name string) bool {
	if name == "" {
		return false
	}
	switch {
	case strings.HasPrefix(name, "loop"),
		strings.HasPrefix(name, "ram"),
		strings.HasPrefix(name, "dm-"),
		strings.HasPrefix(name, "md"),
		strings.HasPrefix(name, "zram"),
		strings.HasPrefix(name, "sr"):
		return false
	}
	// Skip partitions: sda1, nvme0n1p1, mmcblk0p1.
	last := name[len(name)-1]
	if last >= '0' && last <= '9' {
		if strings.HasPrefix(name, "sd") || strings.HasPrefix(name, "hd") ||
			strings.HasPrefix(name, "vd") || strings.HasPrefix(name, "xvd") {
			return false
		}
		if strings.Contains(name, "p") {
			// nvme0n1 (whole) vs nvme0n1p1 (part). Whole ends in a digit but
			// has no 'p' followed by digits at the tail.
			for i := len(name) - 1; i > 0; i-- {
				if name[i] == 'p' && name[i-1] >= '0' && name[i-1] <= '9' {
					return false
				}
				if name[i] < '0' || name[i] > '9' {
					break
				}
			}
		}
	}
	return true
}

func diskMatchesFilter(device string, filter []string) bool {
	for _, f := range filter {
		if strings.Contains(device, f) {
			return true
		}
	}
	return false
}

// physicalDevice strips partition suffix: /dev/sda2 -> /dev/sda, /dev/nvme0n1p1 -> /dev/nvme0n1.
func physicalDevice(device string) string {
	s := device
	for len(s) > 0 && s[len(s)-1] >= '0' && s[len(s)-1] <= '9' {
		s = s[:len(s)-1]
	}
	if strings.HasSuffix(s, "p") && len(s) > 1 {
		prev := s[len(s)-2]
		if prev >= '0' && prev <= '9' {
			s = s[:len(s)-1]
		}
	}
	return s
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
