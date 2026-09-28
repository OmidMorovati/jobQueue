package domain

import (
	"context"
	"encoding/json"
	"time"
)

// JobStatus represents the state of a job
type JobStatus string

const (
	JobStatusPending    JobStatus = "pending"
	JobStatusProcessing JobStatus = "processing"
	JobStatusCompleted  JobStatus = "completed"
	JobStatusFailed     JobStatus = "failed"
)

// Priority levels
type Priority int

const (
	PriorityLow    Priority = 10
	PriorityMedium Priority = 5
	PriorityHigh   Priority = 1
)

// Job represents a unit of work
type Job struct {
	ID          string          `json:"id"`
	Type        string          `json:"type"`
	Payload     json.RawMessage `json:"payload"`
	Priority    Priority        `json:"priority"`
	Status      JobStatus       `json:"status"`
	Queue       string          `json:"queue"`
	CreatedAt   time.Time       `json:"created_at"`
	StartedAt   *time.Time      `json:"started_at,omitempty"`
	CompletedAt *time.Time      `json:"completed_at,omitempty"`
	Error       *string         `json:"error,omitempty"`
	RetryCount  int             `json:"retry_count"`
	MaxRetries  int             `json:"max_retries"`
}

// JobHandler defines the interface for job execution
type JobHandler interface {
	Type() string
	Execute(ctx context.Context, payload json.RawMessage) error
}

// Queue defines the interface for job queue operations
type Queue interface {
	Enqueue(ctx context.Context, job *Job) error
	Dequeue(ctx context.Context, queueName string) (*Job, error)
	Acknowledge(ctx context.Context, job *Job) error
	Nack(ctx context.Context, job *Job, requeue bool) error
}

// Repository defines the interface for job persistence
type Repository interface {
	Save(ctx context.Context, job *Job) error
	GetByID(ctx context.Context, id string) (*Job, error)
	UpdateStatus(ctx context.Context, id string, status JobStatus, errMsg string) error
	List(ctx context.Context, status JobStatus, limit int) ([]*Job, error)
}
