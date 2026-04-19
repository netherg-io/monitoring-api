package docker

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

const apiVersion = "v1.43"

type Client struct {
	hc      *http.Client
	baseURL string
	host    string
}

func New() (*Client, error) {
	sock := os.Getenv("DOCKER_HOST")
	if sock == "" {
		sock = "unix:///var/run/docker.sock"
	}
	u, err := url.Parse(sock)
	if err != nil {
		return nil, fmt.Errorf("invalid DOCKER_HOST %q: %w", sock, err)
	}
	tr := &http.Transport{
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			var d net.Dialer
			switch u.Scheme {
			case "unix":
				return d.DialContext(ctx, "unix", u.Path)
			case "tcp", "http":
				return d.DialContext(ctx, "tcp", u.Host)
			default:
				return nil, fmt.Errorf("unsupported scheme %q", u.Scheme)
			}
		},
		DisableKeepAlives: false,
	}
	return &Client{
		hc:      &http.Client{Transport: tr, Timeout: 15 * time.Second},
		baseURL: "http://docker",
		host:    sock,
	}, nil
}

func (d *Client) Close() error { return nil }

func (d *Client) do(ctx context.Context, method, path string, q url.Values, body io.Reader) (*http.Response, error) {
	u := d.baseURL + "/" + apiVersion + path
	if q != nil {
		u += "?" + q.Encode()
	}
	req, err := http.NewRequestWithContext(ctx, method, u, body)
	if err != nil {
		return nil, err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := d.hc.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode >= 400 {
		b, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		return nil, fmt.Errorf("docker %s %s: %d %s", method, path, resp.StatusCode, strings.TrimSpace(string(b)))
	}
	return resp, nil
}

type Info struct {
	ID      string   `json:"id"`
	Name    string   `json:"name"`
	Image   string   `json:"image"`
	State   string   `json:"state"`
	Status  string   `json:"status"`
	Created int64    `json:"created"`
	Ports   []string `json:"ports,omitempty"`
}

type rawList struct {
	ID      string   `json:"Id"`
	Names   []string `json:"Names"`
	Image   string   `json:"Image"`
	State   string   `json:"State"`
	Status  string   `json:"Status"`
	Created int64    `json:"Created"`
	Ports   []struct {
		PrivatePort int    `json:"PrivatePort"`
		PublicPort  int    `json:"PublicPort"`
		Type        string `json:"Type"`
	} `json:"Ports"`
}

func (d *Client) List(ctx context.Context) ([]Info, error) {
	q := url.Values{}
	q.Set("all", "true")
	resp, err := d.do(ctx, "GET", "/containers/json", q, nil)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	var raw []rawList
	if err := json.NewDecoder(resp.Body).Decode(&raw); err != nil {
		return nil, err
	}
	out := make([]Info, 0, len(raw))
	for _, r := range raw {
		name := ""
		if len(r.Names) > 0 {
			name = strings.TrimPrefix(r.Names[0], "/")
		}
		ports := make([]string, 0, len(r.Ports))
		for _, p := range r.Ports {
			if p.PublicPort != 0 {
				ports = append(ports, fmt.Sprintf("%d:%d/%s", p.PublicPort, p.PrivatePort, p.Type))
			}
		}
		out = append(out, Info{
			ID: r.ID, Name: name, Image: r.Image,
			State: r.State, Status: r.Status,
			Created: r.Created, Ports: ports,
		})
	}
	return out, nil
}

type Stat struct {
	ID         string  `json:"id"`
	Name       string  `json:"name"`
	CPUPercent float64 `json:"cpu"`
	MemUsed    uint64  `json:"mem_used"`
	MemLimit   uint64  `json:"mem_limit"`
	MemPercent float64 `json:"mem"`
	NetRx      uint64  `json:"net_rx"`
	NetTx      uint64  `json:"net_tx"`
	BlockRead  uint64  `json:"blk_read"`
	BlockWrite uint64  `json:"blk_write"`
}

type rawStats struct {
	Read     time.Time `json:"read"`
	Name     string    `json:"name"`
	CPUStats struct {
		CPUUsage struct {
			TotalUsage uint64 `json:"total_usage"`
		} `json:"cpu_usage"`
		SystemCPUUsage uint64 `json:"system_cpu_usage"`
		OnlineCPUs     uint32 `json:"online_cpus"`
	} `json:"cpu_stats"`
	PreCPUStats struct {
		CPUUsage struct {
			TotalUsage uint64 `json:"total_usage"`
		} `json:"cpu_usage"`
		SystemCPUUsage uint64 `json:"system_cpu_usage"`
	} `json:"precpu_stats"`
	MemoryStats struct {
		Usage uint64            `json:"usage"`
		Limit uint64            `json:"limit"`
		Stats map[string]uint64 `json:"stats"`
	} `json:"memory_stats"`
	Networks   map[string]struct{ RxBytes, TxBytes uint64 } `json:"networks"`
	BlkioStats struct {
		IoServiceBytesRecursive []struct {
			Op    string `json:"op"`
			Value uint64 `json:"value"`
		} `json:"io_service_bytes_recursive"`
	} `json:"blkio_stats"`
}

func (d *Client) Stats(ctx context.Context, id string) (Stat, error) {
	q := url.Values{}
	q.Set("stream", "false")
	resp, err := d.do(ctx, "GET", "/containers/"+id+"/stats", q, nil)
	if err != nil {
		return Stat{}, err
	}
	defer resp.Body.Close()
	var r rawStats
	if err := json.NewDecoder(resp.Body).Decode(&r); err != nil {
		return Stat{}, err
	}

	var cpuPct float64
	cpuDelta := float64(r.CPUStats.CPUUsage.TotalUsage) - float64(r.PreCPUStats.CPUUsage.TotalUsage)
	sysDelta := float64(r.CPUStats.SystemCPUUsage) - float64(r.PreCPUStats.SystemCPUUsage)
	if sysDelta > 0 && cpuDelta > 0 {
		cpus := float64(r.CPUStats.OnlineCPUs)
		if cpus == 0 {
			cpus = 1
		}
		cpuPct = (cpuDelta / sysDelta) * cpus * 100.0
	}

	memUsed := r.MemoryStats.Usage
	if cache, ok := r.MemoryStats.Stats["cache"]; ok && cache < memUsed {
		memUsed -= cache
	} else if inactive, ok := r.MemoryStats.Stats["inactive_file"]; ok && inactive < memUsed {
		memUsed -= inactive
	}
	var memPct float64
	if r.MemoryStats.Limit > 0 {
		memPct = float64(memUsed) / float64(r.MemoryStats.Limit) * 100.0
	}

	var rx, tx uint64
	for _, n := range r.Networks {
		rx += n.RxBytes
		tx += n.TxBytes
	}

	var br, bw uint64
	for _, b := range r.BlkioStats.IoServiceBytesRecursive {
		switch strings.ToLower(b.Op) {
		case "read":
			br += b.Value
		case "write":
			bw += b.Value
		}
	}

	return Stat{
		ID: id, Name: strings.TrimPrefix(r.Name, "/"),
		CPUPercent: cpuPct,
		MemUsed:    memUsed, MemLimit: r.MemoryStats.Limit, MemPercent: memPct,
		NetRx: rx, NetTx: tx,
		BlockRead: br, BlockWrite: bw,
	}, nil
}

type inspectResult struct {
	Name   string `json:"Name"`
	Config struct {
		Tty bool `json:"Tty"`
	} `json:"Config"`
}

func (d *Client) inspect(ctx context.Context, id string) (inspectResult, error) {
	resp, err := d.do(ctx, "GET", "/containers/"+id+"/json", nil, nil)
	if err != nil {
		return inspectResult{}, err
	}
	defer resp.Body.Close()
	var r inspectResult
	err = json.NewDecoder(resp.Body).Decode(&r)
	return r, err
}

func (d *Client) Inspect(ctx context.Context, id string) (string, error) {
	r, err := d.inspect(ctx, id)
	if err != nil {
		return "", err
	}
	return strings.TrimPrefix(r.Name, "/"), nil
}

func (d *Client) Logs(ctx context.Context, id string, tail int, since time.Time) ([]string, error) {
	insp, err := d.inspect(ctx, id)
	if err != nil {
		return nil, err
	}

	q := url.Values{}
	q.Set("stdout", "true")
	q.Set("stderr", "true")
	q.Set("timestamps", "true")
	if tail > 0 {
		q.Set("tail", strconv.Itoa(tail))
	} else {
		q.Set("tail", "all")
	}
	if !since.IsZero() {
		q.Set("since", fmt.Sprintf("%d.%09d", since.Unix(), since.Nanosecond()))
	}

	resp, err := d.do(ctx, "GET", "/containers/"+id+"/logs", q, nil)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var buf bytes.Buffer
	if insp.Config.Tty {
		if _, err := io.Copy(&buf, resp.Body); err != nil && !errors.Is(err, io.EOF) {
			return nil, err
		}
	} else {
		if err := demuxStdcopy(resp.Body, &buf); err != nil {
			return nil, err
		}
	}

	s := strings.TrimRight(buf.String(), "\n")
	if s == "" {
		return nil, nil
	}
	return strings.Split(s, "\n"), nil
}

// demuxStdcopy reads Docker's multiplexed log stream (8-byte header + payload)
// and writes stdout/stderr payload into dst, ignoring stream distinction.
func demuxStdcopy(r io.Reader, dst io.Writer) error {
	header := make([]byte, 8)
	for {
		_, err := io.ReadFull(r, header)
		if err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				return nil
			}
			return err
		}
		size := binary.BigEndian.Uint32(header[4:8])
		if size == 0 {
			continue
		}
		if _, err := io.CopyN(dst, r, int64(size)); err != nil {
			return err
		}
	}
}

func (d *Client) Action(ctx context.Context, id, action string) error {
	var path string
	switch action {
	case "start":
		path = "/containers/" + id + "/start"
	case "stop":
		path = "/containers/" + id + "/stop"
	case "restart":
		path = "/containers/" + id + "/restart"
	default:
		return fmt.Errorf("unknown action %q", action)
	}
	q := url.Values{}
	if action != "start" {
		q.Set("t", "10")
	}
	resp, err := d.do(ctx, "POST", path, q, nil)
	if err != nil {
		return err
	}
	resp.Body.Close()
	return nil
}
