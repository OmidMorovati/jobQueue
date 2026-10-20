# 🚀 Distributed Job Queue in Go

A production-ready distributed job queue system built with Go, Redis, and PostgreSQL. Features worker pools, distributed locks, semaphores for rate limiting, retry logic, and a real-time dashboard.

![Go](https://img.shields.io/badge/Go-1.21+-00ADD8?style=flat&logo=go)
![Redis](https://img.shields.io/badge/Redis-7+-DC382D?style=flat&logo=redis)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16+-4169E1?style=flat&logo=postgresql)
![License](https://img.shields.io/badge/License-MIT-green)

## ✨ Features

- 🔄 **Distributed Job Queue** — Redis-backed priority queue with PostgreSQL durability
- 👷 **Worker Pool** — Configurable concurrent job processing with goroutines
- 🔒 **Distributed Locks** — Prevent duplicate job execution across multiple workers
- 🚦 **Semaphores** — Rate limiting for external API calls (email, webhooks, etc.)
- 🔁 **Retry Logic** — Exponential backoff for failed jobs
- 📊 **Real-time Dashboard** — HTMX-powered UI with live job monitoring
- 🎯 **Priority Queues** — High / Medium / Low priority job processing
- 🛡️ **Graceful Shutdown** — Finish current jobs before exiting
- 📝 **Structured Logging** — Production-ready logging with `slog`
- 🔧 **Clean Architecture** — Domain-driven design with interfaces

## 🏗️ Architecture

```
┌─────────────┐
│  HTTP API   │ ← Submit jobs via REST API
└──────┬──────┘
       │
       ├─→ PostgreSQL (Job persistence & audit trail)
       │
       └─→ Redis (Fast priority queue)
              │
              ↓
       ┌─────────────┐
       │ Worker Pool │ ← 10 concurrent workers
       │    (Go)     │
       └──────┬──────┘
              │
              ├─→ Distributed Locks (Prevent duplicates)
              ├─→ Semaphores (Rate limit external APIs)
              └─→ Job Handlers (Business logic)
```

## 🛠️ Tech Stack

**Backend**
- **Go 1.21+** — Concurrent job processing
- **Chi Router** — HTTP API routing
- **pgx** — PostgreSQL driver with connection pooling
- **go-redis** — Redis client for queue operations
- **slog** — Structured logging (stdlib)

**Infrastructure**
- **Redis 7+** — Fast priority queue with sorted sets
- **PostgreSQL 16+** — Durable job storage

## 📋 Prerequisites

- Go 1.21 or higher
- Docker & Docker Compose
- Redis 7+
- PostgreSQL 16+

## 🚀 Quick Start

### 1. Clone the Repository

```bash
git clone https://github.com/yourusername/job-queue.git
cd job-queue
```

### 2. Start Redis & PostgreSQL

```bash
docker-compose up -d
```

### 3. Configure Environment

Create a `.env` file in the project root:

```env
API_PORT=:8080
REDIS_HOST=localhost
REDIS_PORT=6379
REDIS_PASSWORD=
REDIS_DB=0
DB_HOST=localhost
DB_PORT=5432
DB_USER=postgres
DB_PASSWORD=postgres
DB_NAME=jobqueue
DB_SSLMODE=disable
WORKER_CONCURRENCY=10
JOB_TIMEOUT=30s
```

### 4. Run Database Migrations

```bash
go run github.com/pressly/goose/v3/cmd/goose@latest \
  -dir ./migrations \
  postgres "postgres://postgres:postgres@localhost:5432/jobqueue?sslmode=disable" up
```

### 5. Start the API Server

```bash
go run cmd/api/main.go
```

### 6. Start the Worker Pool

In a new terminal:

```bash
go run cmd/worker/main.go
```

### 7. Access the Dashboard

Open your browser: **http://localhost:8080/**

## 📁 Project Structure

```
job-queue/
├── cmd/
│   ├── api/
│   │   └── main.go              # HTTP API server
│   └── worker/
│       └── main.go              # Worker pool process
├── internal/
│   ├── api/
│   │   └── handler/
│   │       ├── job.go           # Job submission handlers
│   │       └── dashboard.go     # Dashboard handlers
│   ├── config/
│   │   └── config.go            # Environment configuration
│   ├── domain/
│   │   └── job.go               # Job models & interfaces
│   ├── handlers/
│   │   ├── email.go             # Email job handler
│   │   └── report.go            # Report generation handler
│   ├── lock/
│   │   └── redis.go             # Distributed lock implementation
│   ├── queue/
│   │   └── redis.go             # Redis queue implementation
│   ├── repository/
│   │   └── job_pgx.go           # PostgreSQL repository
│   ├── semaphore/
│   │   └── limiter.go           # Resource limiter
│   └── worker/
│       ├── pool.go              # Worker pool manager
│       └── tracker.go           # Worker status tracker
├── migrations/
│   └── 001_create_jobs_table.sql
├── web/
│   ├── templates/
│   │   ├── dashboard.html
│   │   └── partials/
│   │       ├── stats_cards.html
│   │       └── jobs_table.html
│   └── static/
├── docker-compose.yml
├── .env
├── go.mod
├── go.sum
└── README.md
```

## 🔑 API Endpoints

### Submit a Job

```bash
curl -X POST http://localhost:8080/jobs \
  -H "Content-Type: application/json" \
  -d '{
    "type": "send_email",
    "payload": {
      "to": "user@example.com",
      "subject": "Welcome!",
      "body": "Thanks for signing up"
    },
    "priority": "high",
    "max_retries": 3
  }'
```

**Response:**

```json
{
  "job_id": "abc-123-def",
  "status": "pending"
}
```

### Get Job Status

```bash
curl http://localhost:8080/jobs/{job_id}
```

### List Jobs

```bash
# All jobs
curl http://localhost:8080/jobs

# Filter by status
curl "http://localhost:8080/jobs?status=pending"
curl "http://localhost:8080/jobs?status=completed"
curl "http://localhost:8080/jobs?status=failed"
```

### Dashboard Endpoints

```bash
# HTML dashboard
curl http://localhost:8080/

# Stats fragment (HTMX)
curl http://localhost:8080/api/stats

# Jobs table fragment (HTMX)
curl http://localhost:8080/api/jobs
```

## 🎯 Job Types

### Send Email Job

```json
{
  "type": "send_email",
  "payload": {
    "to": "user@example.com",
    "subject": "Hello",
    "body": "Message content"
  }
}
```

### Generate Report Job

```json
{
  "type": "generate_report",
  "payload": {
    "report_type": "monthly_sales",
    "user_id": "user-123"
  }
}
```

### Add Your Own Job Type

Create a new handler in `internal/handlers/`:

```go
package handlers

import (
    "context"
    "encoding/json"
)

type MyCustomJob struct{}

func (h *MyCustomJob) Type() string {
    return "my_custom_job"
}

func (h *MyCustomJob) Execute(ctx context.Context, payload json.RawMessage) error {
    var data struct {
        Field1 string `json:"field1"`
        Field2 int    `json:"field2"`
    }

    if err := json.Unmarshal(payload, &data); err != nil {
        return err
    }

    // Your business logic here
    return nil
}
```

Register it in `cmd/worker/main.go`:

```go
registry.Register(&handlers.MyCustomJob{})
```

## 🔐 Concurrency Patterns

### 1. Worker Pool

A fixed number of goroutines process jobs concurrently:

```go
for i := 0; i < concurrency; i++ {
    go func(workerID int) {
        p.runWorker(ctx, workerID)
    }(i)
}
```

**Benefits:**
- Controlled concurrency (prevents resource exhaustion)
- Efficient resource usage
- Easy to monitor and scale

### 2. Distributed Locks

Redis-based locks prevent duplicate job execution across worker instances:

```go
lock := lock.NewDistributedLock(redisClient, jobID, 30*time.Second)
acquired, _ := lock.Acquire(ctx)
defer lock.Release(ctx)
```

**Benefits:**
- Works across multiple worker instances
- Prevents race conditions
- Automatic expiration (prevents deadlocks)

### 3. Semaphores

Limit concurrent access to rate-limited resources:

```go
limiter := semaphore.NewResourceLimiter("email_api", 5)
limiter.Acquire(ctx)
defer limiter.Release()
```

**Benefits:**
- Protects external APIs from rate limits
- Prevents overwhelming downstream services
- Configurable per resource type

### 4. Context Propagation

Cancellation signals flow through the entire call chain:

```go
ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
defer cancel()
handler.Execute(ctx, payload)
```

**Benefits:**
- Graceful shutdown
- Timeout enforcement
- Cancellation propagation

## 🔧 Configuration

### Environment Variables

**API Server**
- `API_PORT` — HTTP server port (default: `:8080`)

**Redis**
- `REDIS_HOST` — Redis host (default: `localhost`)
- `REDIS_PORT` — Redis port (default: `6379`)
- `REDIS_PASSWORD` — Redis password (default: empty)
- `REDIS_DB` — Redis database number (default: `0`)

**PostgreSQL**
- `DB_HOST` — PostgreSQL host (default: `localhost`)
- `DB_PORT` — PostgreSQL port (default: `5432`)
- `DB_USER` — Database user (default: `postgres`)
- `DB_PASSWORD` — Database password (default: `postgres`)
- `DB_NAME` — Database name (default: `jobqueue`)
- `DB_SSLMODE` — SSL mode (default: `disable`)

**Worker**
- `WORKER_CONCURRENCY` — Number of concurrent workers (default: `10`)
- `JOB_TIMEOUT` — Job execution timeout (default: `30s`)

## 🧪 Testing

Run unit tests:

```bash
go test ./... -v
```

Run with race detector:

```bash
go test -race ./...
```

Run a specific package:

```bash
go test ./internal/worker/... -v
```

## 🐳 Docker Deployment

### Build Images

```bash
# API Server
docker build -f Dockerfile.api -t job-queue-api .

# Worker
docker build -f Dockerfile.worker -t job-queue-worker .
```

### Run with Docker Compose

```bash
docker-compose up -d
```

### Scale Workers

```bash
docker-compose up -d --scale worker=5
```

## 📊 Job Lifecycle

```
PENDING → PROCESSING → COMPLETED
                ↓
            (on failure)
                ↓
      retry_count < max_retries?
                ↓
        YES → PENDING (retry with backoff)
        NO  → FAILED (permanent)
```

## 🤝 Contributing

Contributions are welcome! Please follow these steps:

1. Fork the repository
2. Create a feature branch: `git checkout -b feature/amazing-feature`
3. Commit your changes: `git commit -m 'Add amazing feature'`
4. Push to the branch: `git push origin feature/amazing-feature`
5. Open a Pull Request

## 📄 License

This project is licensed under the MIT License.

## 🙏 Acknowledgments

- [Redis](https://redis.io) — For the amazing in-memory data store
- [PostgreSQL](https://www.postgresql.org) — For the powerful relational database
- [HTMX](https://htmx.org) — For making dynamic UIs simple
- [Chi](https://go-chi.github.io/chi/) — For the excellent router

## 📚 Resources

- [Go Concurrency Patterns](https://go.dev/blog/pipelines)
- [Redis Sorted Sets](https://redis.io/docs/data-types/sorted-sets/)
- [Distributed Locks with Redis](https://redis.io/docs/manual/patterns/distributed-locks/)
- [Go Worker Pool Pattern](https://gobyexample.com/worker-pools)

---

**Built with ❤️ using Go**