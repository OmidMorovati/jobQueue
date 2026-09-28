package main

import (
	"context"
	"fmt"
	"github.com/go-chi/chi/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/omidmorovati/jobQueue/internal/api/handler"
	"github.com/omidmorovati/jobQueue/internal/config"
	"github.com/omidmorovati/jobQueue/internal/queue"
	"github.com/omidmorovati/jobQueue/internal/repository"
	"github.com/redis/go-redis/v9"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
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
	jobHandler := handler.NewJobHandler(jobQueue, jobRepo, logger)

	// Setup router
	r := chi.NewRouter()
	r.Post("/jobs", jobHandler.CreateJob)
	r.Get("/jobs/{jobID}", jobHandler.GetJob)
	r.Get("/jobs", jobHandler.ListJobs)

	// Start server
	srv := &http.Server{Addr: cfg.APIPort, Handler: r}
	go func() {
		logger.Info("API server starting", "port", cfg.APIPort)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("server crashed", "error", err)
			os.Exit(1)
		}
	}()

	// Graceful shutdown
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	logger.Info("shutting down API server...")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	srv.Shutdown(ctx)
}
