package worker

import (
	"context"
	"fmt"
	"github.com/omidmorovati/jobQueue/internal/domain"
	"log/slog"
	"sync"
	"time"
)

// Registry maps job types to handlers
type Registry struct {
	handlers map[string]domain.JobHandler
}

func NewRegistry() *Registry {
	return &Registry{handlers: make(map[string]domain.JobHandler)}
}

func (r *Registry) Register(handler domain.JobHandler) {
	r.handlers[handler.Type()] = handler
}

func (r *Registry) Get(jobType string) (domain.JobHandler, bool) {
	handler, ok := r.handlers[jobType]
	return handler, ok
}

// Pool manages multiple workers
type Pool struct {
	queue       domain.Queue
	repo        domain.Repository
	registry    *Registry
	concurrency int
	timeout     time.Duration
	logger      *slog.Logger
}

func NewPool(queue domain.Queue, repo domain.Repository, registry *Registry, concurrency int, timeout time.Duration, logger *slog.Logger) *Pool {
	return &Pool{
		queue:       queue,
		repo:        repo,
		registry:    registry,
		concurrency: concurrency,
		timeout:     timeout,
		logger:      logger,
	}
}

func (p *Pool) Start(ctx context.Context) error {
	var wg sync.WaitGroup

	// Start worker goroutines
	for i := 0; i < p.concurrency; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			p.runWorker(ctx, workerID)
		}(i)
	}

	// Wait for context cancellation
	<-ctx.Done()
	p.logger.Info("shutting down worker pool...")

	// Wait for all workers to finish current jobs
	wg.Wait()
	return nil
}

func (p *Pool) runWorker(ctx context.Context, workerID int) {
	p.logger.Info("worker started", "worker_id", workerID)

	for {
		select {
		case <-ctx.Done():
			return
		default:
			// Try to dequeue a job
			job, err := p.queue.Dequeue(ctx, "default")
			if err != nil {
				p.logger.Error("dequeue failed", "error", err, "worker_id", workerID)
				time.Sleep(1 * time.Second) // Backoff on error
				continue
			}

			if job == nil {
				// Queue is empty, wait a bit
				time.Sleep(100 * time.Millisecond)
				continue
			}

			// Process the job
			p.processJob(ctx, job, workerID)
		}
	}
}

func (p *Pool) processJob(ctx context.Context, job *domain.Job, workerID int) {
	p.logger.Info("processing job", "job_id", job.ID, "type", job.Type, "worker_id", workerID)

	// Update status to processing
	if err := p.repo.UpdateStatus(ctx, job.ID, domain.JobStatusProcessing, ""); err != nil {
		p.logger.Error("update status failed", "error", err, "job_id", job.ID)
	}

	// Get handler for job type
	handler, ok := p.registry.Get(job.Type)
	if !ok {
		errMsg := fmt.Sprintf("no handler registered for job type: %s", job.Type)
		p.logger.Error(errMsg, "job_id", job.ID)
		p.repo.UpdateStatus(ctx, job.ID, domain.JobStatusFailed, errMsg)
		return
	}

	// Execute with timeout
	jobCtx, cancel := context.WithTimeout(ctx, p.timeout)
	defer cancel()

	err := handler.Execute(jobCtx, job.Payload)

	if err != nil {
		p.logger.Error("job failed", "job_id", job.ID, "error", err, "retry_count", job.RetryCount)

		if job.RetryCount < job.MaxRetries {
			backoff := time.Duration(job.RetryCount+1) * time.Second
			time.Sleep(backoff)
			p.queue.Nack(ctx, job, true)
		} else {
			errMsg := err.Error()
			p.repo.UpdateStatus(ctx, job.ID, domain.JobStatusFailed, errMsg) // UpdateStatus already takes string
			p.queue.Acknowledge(ctx, job)
		}
	} else {
		p.logger.Info("job completed", "job_id", job.ID)
		p.repo.UpdateStatus(ctx, job.ID, domain.JobStatusCompleted, "")
		p.queue.Acknowledge(ctx, job)
	}
}
