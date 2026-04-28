package tsdb

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"monitoring-api/internal/buffer"
)

type Store struct {
	pool   *pgxpool.Pool
	hostID string
}

const schema = `
CREATE EXTENSION IF NOT EXISTS timescaledb;

CREATE TABLE IF NOT EXISTS metrics (
	ts       TIMESTAMPTZ NOT NULL,
	host_id  TEXT        NOT NULL,
	payload  JSONB       NOT NULL
);
SELECT create_hypertable('metrics', 'ts', if_not_exists => TRUE);
CREATE INDEX IF NOT EXISTS metrics_host_ts_idx ON metrics (host_id, ts DESC);

CREATE TABLE IF NOT EXISTS request_counts (
	ts       TIMESTAMPTZ NOT NULL,
	host_id  TEXT        NOT NULL,
	count    BIGINT      NOT NULL,
	PRIMARY KEY (host_id, ts)
);
SELECT create_hypertable('request_counts', 'ts', if_not_exists => TRUE);
`

func Open(ctx context.Context, dsn, hostID string) (*Store, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("pgx pool: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping: %w", err)
	}
	if _, err := pool.Exec(ctx, schema); err != nil {
		pool.Close()
		return nil, fmt.Errorf("schema: %w", err)
	}
	return &Store{pool: pool, hostID: hostID}, nil
}

func (s *Store) Close() {
	if s != nil && s.pool != nil {
		s.pool.Close()
	}
}

func (s *Store) WriteSnapshot(ctx context.Context, p buffer.Point) error {
	payload, err := json.Marshal(p)
	if err != nil {
		return err
	}
	_, err = s.pool.Exec(ctx,
		`INSERT INTO metrics (ts, host_id, payload) VALUES ($1, $2, $3)`,
		p.TS, s.hostID, payload)
	return err
}

// QueryRange returns metrics for the host since `since`, ordered chronologically.
func (s *Store) QueryRange(ctx context.Context, since time.Time) ([]buffer.Point, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT payload FROM metrics WHERE host_id=$1 AND ts >= $2 ORDER BY ts ASC`,
		s.hostID, since)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []buffer.Point
	for rows.Next() {
		var raw []byte
		if err := rows.Scan(&raw); err != nil {
			return nil, err
		}
		var p buffer.Point
		if err := json.Unmarshal(raw, &p); err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

// UpsertRequestCount writes the count for a given minute bucket.
func (s *Store) UpsertRequestCount(ctx context.Context, minute time.Time, count int64) error {
	_, err := s.pool.Exec(ctx,
		`INSERT INTO request_counts (ts, host_id, count) VALUES ($1, $2, $3)
		 ON CONFLICT (host_id, ts) DO UPDATE SET count = EXCLUDED.count`,
		minute, s.hostID, count)
	return err
}

type RequestBucket struct {
	Minute time.Time
	Count  int64
}

func (s *Store) QueryRequests(ctx context.Context, since time.Time) ([]RequestBucket, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT ts, count FROM request_counts WHERE host_id=$1 AND ts >= $2 ORDER BY ts ASC`,
		s.hostID, since)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []RequestBucket
	for rows.Next() {
		var b RequestBucket
		if err := rows.Scan(&b.Minute, &b.Count); err != nil {
			return nil, err
		}
		out = append(out, b)
	}
	return out, rows.Err()
}
