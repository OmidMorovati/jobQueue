package semaphore

import (
	"context"
	"golang.org/x/sync/semaphore"
	"time"
)

type ResourceLimiter struct {
	sem  *semaphore.Weighted
	name string
}

func NewResourceLimiter(name string, maxConcurrent int64) *ResourceLimiter {
	return &ResourceLimiter{
		sem:  semaphore.NewWeighted(maxConcurrent),
		name: name,
	}
}

// Acquire blocks until a permit is available
func (r *ResourceLimiter) Acquire(ctx context.Context) error {
	return r.sem.Acquire(ctx, 1)
}

// TryAcquire returns immediately (true if acquired, false if full)
func (r *ResourceLimiter) TryAcquire() bool {
	return r.sem.TryAcquire(1)
}

// Release returns a permit
func (r *ResourceLimiter) Release() {
	r.sem.Release(1)
}

// WaitTimeout waits with a timeout
func (r *ResourceLimiter) WaitTimeout(ctx context.Context, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	return r.sem.Acquire(ctx, 1)
}
