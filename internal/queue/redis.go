package queue

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/omidmorovati/jobQueue/internal/domain"
	"github.com/redis/go-redis/v9"
)

type RedisQueue struct {
	client *redis.Client
}

func NewRedisQueue(cfg *redis.Options) *RedisQueue {
	client := redis.NewClient(cfg)
	return &RedisQueue{client: client}
}

func (q *RedisQueue) Enqueue(ctx context.Context, job *domain.Job) error {
	// Serialize job to JSON
	data, err := json.Marshal(job)
	if err != nil {
		return fmt.Errorf("marshal job: %w", err)
	}

	// Use sorted set for priority queue (lower score = higher priority)
	queueKey := fmt.Sprintf("queue:%s", job.Queue)
	score := float64(job.Priority)*1000 + float64(job.CreatedAt.UnixMilli())/1000000

	return q.client.ZAdd(ctx, queueKey, redis.Z{
		Score:  score,
		Member: string(data),
	}).Err()
}

func (q *RedisQueue) Dequeue(ctx context.Context, queueName string) (*domain.Job, error) {
	queueKey := fmt.Sprintf("queue:%s", queueName)

	// Atomically pop the highest priority job (lowest score)
	result, err := q.client.ZPopMin(ctx, queueKey, 1).Result()
	if err != nil {
		return nil, fmt.Errorf("pop from queue: %w", err)
	}

	if len(result) == 0 {
		return nil, nil // Queue is empty
	}

	// Deserialize job
	jobData := result[0].Member.(string)
	var job domain.Job
	if err := json.Unmarshal([]byte(jobData), &job); err != nil {
		return nil, fmt.Errorf("unmarshal job: %w", err)
	}

	return &job, nil
}

func (q *RedisQueue) Acknowledge(ctx context.Context, job *domain.Job) error {
	// Job completed successfully - nothing to do in Redis
	// Status is updated in PostgreSQL
	return nil
}

func (q *RedisQueue) Nack(ctx context.Context, job *domain.Job, requeue bool) error {
	if !requeue {
		return nil // Job goes to dead letter queue (handled in worker)
	}

	// Requeue with incremented retry count
	job.RetryCount++
	return q.Enqueue(ctx, job)
}

func (q *RedisQueue) Close() error {
	return q.client.Close()
}

func (q *RedisQueue) QueueLength(ctx context.Context, queueName string) (int64, error) {
	queueKey := fmt.Sprintf("queue:%s", queueName)
	return q.client.ZCard(ctx, queueKey).Result()
}
