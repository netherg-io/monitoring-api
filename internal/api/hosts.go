package api

import (
	"net/http"
	"os"
)

// Host describes a monitored host. The API is shaped as an array so a future
// multi-host setup is a drop-in change without breaking the client.
type Host struct {
	ID      string `json:"id"`
	Name    string `json:"name"`
	URL     string `json:"url"`
	Current bool   `json:"current"`
}

// hostIdentity returns (id, name) for this backend, falling back to the OS hostname.
func hostIdentity() (string, string) {
	id := os.Getenv("HOST_ID")
	name := os.Getenv("HOST_NAME")
	if name == "" {
		if h, err := os.Hostname(); err == nil {
			name = h
		} else {
			name = "local"
		}
	}
	if id == "" {
		id = name
	}
	return id, name
}

func handleHosts() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id, name := hostIdentity()
		writeJSON(w, 200, []Host{
			{ID: id, Name: name, URL: "", Current: true},
		})
	}
}
