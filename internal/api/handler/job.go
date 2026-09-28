package handler

import (
	"encoding/json"
	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/omidmorovati/jobQueue/internal/domain"
	"log/slog"
	"net/http"
	"time"
)

type JobHandler struct {
	queue  domain.Queue
	repo   domain.Repository
	logger *slog.Logger
}

func NewJobHandler(queue domain.Queue, repo domain.Repository, logger *slog.Logger) *JobHandler {
	return &JobHandler{queue: queue, repo: repo, logger: logger}
}

type CreateJobRequest struct {
	Type       string          `json:"type"`
	Payload    json.RawMessage `json:"payload"`
	Priority   string          `json:"priority"`
	Queue      string          `json:"queue"`
	MaxRetries int             `json:"max_retries"`
}

func (h *JobHandler) CreateJob(w http.ResponseWriter, r *http.Request) {
	var req CreateJobRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}

	// Parse priority
	priority := domain.PriorityMedium
	switch req.Priority {
	case "high":
		priority = domain.PriorityHigh
	case "low":
		priority = domain.PriorityLow
	}

	// Set defaults
	if req.Queue == "" {
		req.Queue = "default"
	}
	if req.MaxRetries == 0 {
		req.MaxRetries = 3
	}

	// Create job
	job := &domain.Job{
		ID:         uuid.New().String(),
		Type:       req.Type,
		Payload:    req.Payload,
		Priority:   priority,
		Status:     domain.JobStatusPending,
		Queue:      req.Queue,
		CreatedAt:  time.Now(),
		MaxRetries: req.MaxRetries,
	}

	// Save to database
	if err := h.repo.Save(r.Context(), job); err != nil {
		h.logger.Error("save job failed", "error", err)
		http.Error(w, "failed to save job", http.StatusInternalServerError)
		return
	}

	// Enqueue to Redis
	if err := h.queue.Enqueue(r.Context(), job); err != nil {
		h.logger.Error("enqueue job failed", "error", err)
		http.Error(w, "failed to enqueue job", http.StatusInternalServerError)
		return
	}

	h.logger.Info("job created", "job_id", job.ID, "type", job.Type)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(map[string]string{
		"job_id": job.ID,
		"status": string(job.Status),
	})
}

func (h *JobHandler) GetJob(w http.ResponseWriter, r *http.Request) {
	jobID := chi.URLParam(r, "jobID")

	job, err := h.repo.GetByID(r.Context(), jobID)
	if err != nil {
		http.Error(w, "job not found", http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(job)
}

func (h *JobHandler) ListJobs(w http.ResponseWriter, r *http.Request) {
	status := domain.JobStatus(r.URL.Query().Get("status"))
	if status == "" {
		status = domain.JobStatusPending
	}

	jobs, err := h.repo.List(r.Context(), status, 50)
	if err != nil {
		h.logger.Error("list jobs failed", "error", err)
		http.Error(w, "failed to list jobs", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(jobs)
}
