package lock

import (
	"context"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"time"
)

type DistributedLock struct {
	client *redis.Client
	key    string
	value  string // Unique identifier for this lock holder
	ttl    time.Duration
}

// NewDistributedLock creates a lock with a unique ID
func NewDistributedLock(client *redis.Client, key string, ttl time.Duration) *DistributedLock {
	return &DistributedLock{
		client: client,
		key:    key,
		value:  uuid.New().String(),
		ttl:    ttl,
	}
}

// Acquire attempts to acquire the lock
// Returns true if successful, false if already held by another worker
func (l *DistributedLock) Acquire(ctx context.Context) (bool, error) {
	// SET key value NX EX ttl
	// NX = Only set if key doesn't exist (atomic)
	// EX = Set expiration (prevents deadlocks)
	result, err := l.client.SetNX(ctx, l.key, l.value, l.ttl).Result()
	return result, err
}

// Release safely releases the lock ONLY if we own it
func (l *DistributedLock) Release(ctx context.Context) error {
	// Use Lua script for atomic check-and-delete
	// Prevents race condition where lock expires and another worker acquires it
	script := `
		if redis.call("GET", KEYS[1]) == ARGV[1] then
			return redis.call("DEL", KEYS[1])
		else
			return 0
		end
	`
	_, err := l.client.Eval(ctx, script, []string{l.key}, l.value).Result()
	return err
}

// Renew extends the lock TTL (for long-running jobs)
func (l *DistributedLock) Renew(ctx context.Context) error {
	script := `
		if redis.call("GET", KEYS[1]) == ARGV[1] then
			return redis.call("EXPIRE", KEYS[1], ARGV[2])
		else
			return 0
		end
	`
	_, err := l.client.Eval(ctx, script, []string{l.key}, l.value, int(l.ttl.Seconds())).Result()
	return err
}
