package cloudflare

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

type Client struct {
	token     string
	accountID string
	tunnelID  string
	hc        *http.Client

	mu      sync.RWMutex
	mapping map[string][]string
	ready   bool
}

func New(token, accountID, tunnelID string) *Client {
	return &Client{
		token:     token,
		accountID: accountID,
		tunnelID:  tunnelID,
		hc:        &http.Client{Timeout: 15 * time.Second},
		mapping:   map[string][]string{},
	}
}

func (c *Client) Enabled() bool {
	return c.token != "" && c.accountID != "" && c.tunnelID != ""
}

func (c *Client) All() map[string][]string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	out := make(map[string][]string, len(c.mapping))
	for k, v := range c.mapping {
		cp := make([]string, len(v))
		copy(cp, v)
		out[k] = cp
	}
	return out
}

func (c *Client) Run(ctx context.Context, interval time.Duration) {
	if !c.Enabled() {
		return
	}
	if interval < 30*time.Second {
		interval = 60 * time.Second
	}
	if err := c.refresh(ctx); err != nil {
		log.Printf("cloudflare: initial refresh failed: %v", err)
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if err := c.refresh(ctx); err != nil {
				log.Printf("cloudflare: refresh failed: %v", err)
			}
		}
	}
}

type ingressRule struct {
	Hostname string `json:"hostname"`
	Service  string `json:"service"`
	Path     string `json:"path"`
}

type tunnelConfigResp struct {
	Success bool `json:"success"`
	Errors  []struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
	} `json:"errors"`
	Result struct {
		Config struct {
			Ingress []ingressRule `json:"ingress"`
		} `json:"config"`
	} `json:"result"`
}

func (c *Client) refresh(ctx context.Context) error {
	u := fmt.Sprintf("https://api.cloudflare.com/client/v4/accounts/%s/cfd_tunnel/%s/configurations",
		url.PathEscape(c.accountID), url.PathEscape(c.tunnelID))
	req, err := http.NewRequestWithContext(ctx, "GET", u, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+c.token)
	req.Header.Set("Accept", "application/json")

	resp, err := c.hc.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		b, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("cf api %d: %s", resp.StatusCode, strings.TrimSpace(string(b)))
	}
	var tr tunnelConfigResp
	if err := json.NewDecoder(resp.Body).Decode(&tr); err != nil {
		return err
	}
	if !tr.Success {
		msg := "unknown"
		if len(tr.Errors) > 0 {
			msg = tr.Errors[0].Message
		}
		return fmt.Errorf("cf api error: %s", msg)
	}

	m := map[string]map[string]struct{}{}
	for _, r := range tr.Result.Config.Ingress {
		if r.Hostname == "" || r.Service == "" {
			continue
		}
		name := extractContainerName(r.Service)
		if name == "" {
			continue
		}
		pub := "https://" + r.Hostname
		if r.Path != "" {
			pub += "/" + strings.TrimPrefix(r.Path, "/")
		}
		if _, ok := m[name]; !ok {
			m[name] = map[string]struct{}{}
		}
		m[name][pub] = struct{}{}
	}

	out := make(map[string][]string, len(m))
	for name, urls := range m {
		list := make([]string, 0, len(urls))
		for u := range urls {
			list = append(list, u)
		}
		out[name] = list
	}

	c.mu.Lock()
	c.mapping = out
	c.ready = true
	c.mu.Unlock()
	return nil
}

// extractContainerName parses a cloudflared ingress service like
// "http://foo:8080", "https://bar", "tcp://baz:22" and returns the host part
// ("foo"). Returns "" for non-address services like "http_status:404".
func extractContainerName(service string) string {
	i := strings.Index(service, "://")
	if i < 0 {
		return ""
	}
	rest := service[i+3:]
	if j := strings.IndexAny(rest, "/?#"); j >= 0 {
		rest = rest[:j]
	}
	if k := strings.LastIndex(rest, ":"); k >= 0 {
		rest = rest[:k]
	}
	return strings.TrimSpace(rest)
}
