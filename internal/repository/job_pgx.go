package repository

import (
	"context"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/omidmorovati/jobQueue/internal/domain"
	"time"
)

type JobRepo struct {
	pool *pgxpool.Pool
}

func NewJobRepo(pool *pgxpool.Pool) *JobRepo {
	return &JobRepo{pool: pool}
}

func (r *JobRepo) Save(ctx context.Context, job *domain.Job) error {
	_, err := r.pool.Exec(ctx,
		`INSERT INTO jobs (id, type, payload, priority, status, queue, created_at, retry_count, max_retries)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
		job.ID, job.Type, job.Payload, job.Priority, job.Status, job.Queue,
		job.CreatedAt, job.RetryCount, job.MaxRetries,
	)
	return err
}

func (r *JobRepo) GetByID(ctx context.Context, id string) (*domain.Job, error) {
	var job domain.Job
	err := r.pool.QueryRow(ctx,
		`SELECT id, type, payload, priority, status, queue, created_at, started_at, completed_at, error, retry_count, max_retries
		 FROM jobs WHERE id = $1`,
		id,
	).Scan(&job.ID, &job.Type, &job.Payload, &job.Priority, &job.Status, &job.Queue,
		&job.CreatedAt, &job.StartedAt, &job.CompletedAt, &job.Error, &job.RetryCount, &job.MaxRetries)

	if err != nil {
		return nil, err
	}
	return &job, nil
}

func (r *JobRepo) UpdateStatus(ctx context.Context, id string, status domain.JobStatus, errMsg string) error {
	var startedAt, completedAt *time.Time
	var errPtr *string
	now := time.Now()

	if status == domain.JobStatusProcessing {
		startedAt = &now
	} else if status == domain.JobStatusCompleted || status == domain.JobStatusFailed {
		completedAt = &now
	}

	if errMsg != "" {
		errPtr = &errMsg
	}

	_, err := r.pool.Exec(ctx,
		`UPDATE jobs SET status = $1, error = $2, started_at = COALESCE($3, started_at), completed_at = COALESCE($4, completed_at)
		 WHERE id = $5`,
		status, errPtr, startedAt, completedAt, id,
	)
	return err
}

func (r *JobRepo) List(ctx context.Context, status domain.JobStatus, limit int) ([]*domain.Job, error) {
	rows, err := r.pool.Query(ctx,
		`SELECT id, type, payload, priority, status, queue, created_at, started_at, completed_at, error, retry_count, max_retries
		 FROM jobs WHERE status = $1 ORDER BY created_at DESC LIMIT $2`,
		status, limit,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var jobs []*domain.Job
	for rows.Next() {
		var job domain.Job
		if err := rows.Scan(&job.ID, &job.Type, &job.Payload, &job.Priority, &job.Status, &job.Queue,
			&job.CreatedAt, &job.StartedAt, &job.CompletedAt, &job.Error, &job.RetryCount, &job.MaxRetries); err != nil {
			return nil, err
		}
		jobs = append(jobs, &job)
	}
	return jobs, rows.Err()
}
