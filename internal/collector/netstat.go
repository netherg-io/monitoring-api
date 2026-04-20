package collector

import (
	"bufio"
	"os"
	"strconv"
	"strings"
)

// hostNetCounters reads /proc/1/net/dev (of PID 1 in the shared PID namespace,
// i.e. the host's init) and sums RX/TX bytes across physical interfaces.
// Virtual interfaces (loopback, docker/bridge/veth/tun/tap/container overlays)
// are skipped so the total reflects real uplink traffic.
//
// Requires docker-compose `pid: host` and a read-only mount of host /proc at
// HOST_PROC (default /host/proc). Returns ok=false when the file cannot be
// parsed, so the caller can fall back to gopsutil.
func hostNetCounters() (rx, tx uint64, ok bool) {
	procPath := os.Getenv("HOST_PROC")
	if procPath == "" {
		procPath = "/host/proc"
	}
	path := procPath + "/1/net/dev"
	f, err := os.Open(path)
	if err != nil {
		return 0, 0, false
	}
	defer f.Close()

	s := bufio.NewScanner(f)
	lineNum := 0
	for s.Scan() {
		lineNum++
		if lineNum <= 2 {
			continue // skip two header lines
		}
		line := s.Text()
		idx := strings.Index(line, ":")
		if idx < 0 {
			continue
		}
		name := strings.TrimSpace(line[:idx])
		if isVirtualIface(name) {
			continue
		}
		fields := strings.Fields(line[idx+1:])
		if len(fields) < 16 {
			continue
		}
		r, err1 := strconv.ParseUint(fields[0], 10, 64)
		t, err2 := strconv.ParseUint(fields[8], 10, 64)
		if err1 != nil || err2 != nil {
			continue
		}
		rx += r
		tx += t
		ok = true
	}
	if err := s.Err(); err != nil {
		return 0, 0, false
	}
	return rx, tx, ok
}

// isVirtualIface reports whether an interface name belongs to a virtual/local
// device that should be excluded from "host uplink" totals.
func isVirtualIface(name string) bool {
	if name == "" || name == "lo" {
		return true
	}
	prefixes := []string{
		"docker", "br-", "veth", "virbr", "vmnet", "vnet",
		"cni", "flannel", "cali", "weave", "kube",
		"tun", "tap",
	}
	for _, p := range prefixes {
		if strings.HasPrefix(name, p) {
			return true
		}
	}
	return false
}
