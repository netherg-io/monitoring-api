package rediscache

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

type Store struct {
	c      *redis.Client
	hostID string
}

func Open(ctx context.Context, addr, hostID string) (*Store, error) {
	c := redis.NewClient(&redis.Options{Addr: addr})
	if err := c.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("redis ping: %w", err)
	}
	return &Store{c: c, hostID: hostID}, nil
}

func (s *Store) Close() {
	if s != nil && s.c != nil {
		_ = s.c.Close()
	}
}

func (s *Store) reqKey(minute int64) string {
	return fmt.Sprintf("requests:%s:%d", s.hostID, minute)
}

// IncrRequest bumps the counter for the minute bucket of `now`.
// Each minute bucket is kept for 13h so a 12h /api/requests query can still hit Redis.
func (s *Store) IncrRequest(ctx context.Context, now time.Time) error {
	minute := now.Unix() - now.Unix()%60
	pipe := s.c.Pipeline()
	pipe.Incr(ctx, s.reqKey(minute))
	pipe.Expire(ctx, s.reqKey(minute), 13*time.Hour)
	_, err := pipe.Exec(ctx)
	return err
}

// GetRequestCount returns the count for a specific minute bucket (0 if missing).
func (s *Store) GetRequestCount(ctx context.Context, minute time.Time) (int64, error) {
	v, err := s.c.Get(ctx, s.reqKey(minute.Unix()-minute.Unix()%60)).Result()
	if err == redis.Nil {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	n, _ := strconv.ParseInt(v, 10, 64)
	return n, nil
}

// GetRequestRange fetches counts for all minute buckets in [from,to] inclusive.
// Missing buckets are returned as zero.
func (s *Store) GetRequestRange(ctx context.Context, from, to time.Time) (map[int64]int64, error) {
	fromMin := from.Unix() - from.Unix()%60
	toMin := to.Unix() - to.Unix()%60
	if toMin < fromMin {
		return map[int64]int64{}, nil
	}
	keys := make([]string, 0, (toMin-fromMin)/60+1)
	for m := fromMin; m <= toMin; m += 60 {
		keys = append(keys, s.reqKey(m))
	}
	vals, err := s.c.MGet(ctx, keys...).Result()
	if err != nil {
		return nil, err
	}
	out := make(map[int64]int64, len(vals))
	for i, v := range vals {
		minute := fromMin + int64(i)*60
		if v == nil {
			out[minute] = 0
			continue
		}
		s, _ := v.(string)
		n, _ := strconv.ParseInt(s, 10, 64)
		out[minute] = n
	}
	return out, nil
}
