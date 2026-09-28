package main

import (
	"context"
	"fmt"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/omidmorovati/jobQueue/internal/config"
	"github.com/omidmorovati/jobQueue/internal/handlers"
	"github.com/omidmorovati/jobQueue/internal/queue"
	"github.com/omidmorovati/jobQueue/internal/repository"
	"github.com/omidmorovati/jobQueue/internal/worker"
	"github.com/redis/go-redis/v9"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	cfg, err := config.Load()
	if err != nil {
		logger.Error("load config failed", "error", err)
		os.Exit(1)
	}

	// Connect to PostgreSQL
	dbURL := fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=%s",
		cfg.DBUser, cfg.DBPassword, cfg.DBHost, cfg.DBPort, cfg.DBName, cfg.DBSSLMode)

	pool, err := pgxpool.New(context.Background(), dbURL)
	if err != nil {
		logger.Error("connect to database failed", "error", err)
		os.Exit(1)
	}
	defer pool.Close()

	// Connect to Redis
	redisClient := redis.NewClient(&redis.Options{
		Addr:     fmt.Sprintf("%s:%s", cfg.RedisHost, cfg.RedisPort),
		Password: cfg.RedisPassword,
		DB:       cfg.RedisDB,
	})
	defer redisClient.Close()

	// Initialize components
	jobQueue := queue.NewRedisQueue(&redis.Options{
		Addr:     fmt.Sprintf("%s:%s", cfg.RedisHost, cfg.RedisPort),
		Password: cfg.RedisPassword,
		DB:       cfg.RedisDB,
	})
	jobRepo := repository.NewJobRepo(pool)

	// Register job handlers
	registry := worker.NewRegistry()
	registry.Register(handlers.NewSendEmailJob(logger))
	registry.Register(handlers.NewGenerateReportJob(logger))

	// Create and start worker pool
	pool := worker.NewPool(jobQueue, jobRepo, registry, cfg.WorkerConcurrency, cfg.JobTimeout, logger)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle shutdown signals
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-quit
		logger.Info("shutting down worker pool...")
		cancel()
	}()

	logger.Info("worker pool starting", "concurrency", cfg.WorkerConcurrency)
	if err := pool.Start(ctx); err != nil {
		logger.Error("worker pool error", "error", err)
		os.Exit(1)
	}
}
