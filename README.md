# LiveDock Monitoring API

LiveDock Monitoring API collects host and container telemetry, stores recent history, and exposes the data that the LiveDock dashboard renders. It is the backend half of the product: token-protected, self-hosted, and intentionally small.

[![Go](https://img.shields.io/badge/Go-1.23-00ADD8?logo=go&logoColor=white)](https://go.dev/)
[![Docker](https://img.shields.io/badge/Docker-ready-2496ED?logo=docker&logoColor=white)](https://www.docker.com/)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](./LICENSE)

## Responsibilities

- Collect host metrics and container state
- Expose a compact JSON API for the dashboard
- Serve request counters and historical snapshots
- Support optional TSDB and Redis-backed persistence
- Allow controlled container actions where permitted

## API

All protected routes require `Authorization: Bearer <API_TOKEN>`.

| Method | Path | Purpose |
| --- | --- | --- |
| `GET` | `/health` | Basic health check |
| `GET` | `/api/snapshot` | Current host snapshot |
| `GET` | `/api/history?minutes=15` | Historical host metrics |
| `GET` | `/api/containers` | Container list |
| `GET` | `/api/containers/{id}/history` | Container history |
| `GET` | `/api/containers/{id}/logs?tail=200` | Recent container logs |
| `POST` | `/api/containers/{id}/action` | `start`, `stop`, or `restart` a container |
| `GET` | `/api/requests` | Request throughput and aggregates |
| `GET` | `/api/hosts` | Registered host identifiers |

## Environment

Required:

- `API_TOKEN` - bearer token used by the dashboard and API clients

Optional:

- `LISTEN_ADDR` - listen address, default `:8080`
- `COLLECT_INTERVAL` - collector interval, default `2s`
- `BUFFER_POINTS` - number of buffered points, default `1800`
- `CONTROL_ALLOWLIST` - comma-separated container names that may be controlled
- `CONTROL_DENYLIST` - comma-separated container names that may never be controlled
- `CORS_ORIGINS` - comma-separated allowed origins
- `HOST_ID` - explicit host identifier, otherwise the hostname is used
- `TSDB_DSN` - optional TSDB connection string
- `REDIS_ADDR` - optional Redis address for request metrics

## Getting started

```bash
go test ./...
go build ./...
```

## Add it to Docker

The published image can be added as a service in your own Docker Compose stack. The API needs access to the Docker daemon so it can list containers, read stats, fetch logs, and run allowed control actions.

```yaml
services:
  livedock-api:
    image: ghcr.io/netherg-io/livedock-api:latest
    container_name: livedock-api
    restart: unless-stopped
    ports:
      - "8080:8080"
    environment:
      API_TOKEN: "replace-me"
      LISTEN_ADDR: ":8080"
      CORS_ORIGINS: "https://homepage-opal-pi.vercel.app"
      HOST_ID: "my-server"
    volumes:
      - /var/run/docker.sock:/var/run/docker.sock
```

Point the dashboard at `http://your-host:8080` and use the same `API_TOKEN` value.

### Optional dependencies

LiveDock API works without external storage by keeping recent points in memory. Add these services only when you want longer history or request counters to survive restarts.

```yaml
services:
  livedock-api:
    image: ghcr.io/netherg-io/livedock-api:latest
    restart: unless-stopped
    ports:
      - "8080:8080"
    environment:
      API_TOKEN: "replace-me"
      REDIS_ADDR: "redis:6379"
      TSDB_DSN: "postgres://livedock:livedock@timescaledb:5432/livedock?sslmode=disable"
    volumes:
      - /var/run/docker.sock:/var/run/docker.sock
    depends_on:
      - redis
      - timescaledb

  redis:
    image: redis:7-alpine
    restart: unless-stopped

  timescaledb:
    image: timescale/timescaledb:latest-pg16
    restart: unless-stopped
    environment:
      POSTGRES_DB: livedock
      POSTGRES_USER: livedock
      POSTGRES_PASSWORD: livedock
    volumes:
      - livedock-timescale:/var/lib/postgresql/data

volumes:
  livedock-timescale:
```

### Build locally

```bash
docker build -t livedock-monitoring-api .
docker run --rm \
  -e API_TOKEN=replace-me \
  -p 8080:8080 \
  livedock-monitoring-api
```

## Code layout

- `main.go` - process bootstrap and dependency wiring
- `internal/api` - HTTP routes and middleware
- `internal/buffer` - in-memory metric buffers
- `internal/collector` - host and container polling
- `internal/docker` - Docker client wrapper
- `internal/storage` - TSDB and Redis persistence layers

## Related app

- [`LiveDock_Dashboard`](https://github.com/netherguy4/LiveDock_Dashboard) - the LiveDock dashboard frontend

## License

MIT
