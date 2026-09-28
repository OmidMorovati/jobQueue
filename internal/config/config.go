package config

import (
	"github.com/caarlos0/env/v6"
	"github.com/joho/godotenv"
	"log"
	"time"
)

type Config struct {
	// API Server
	APIPort string `env:"API_PORT" envDefault:":8080"`

	// Redis
	RedisHost     string `env:"REDIS_HOST" envDefault:"localhost"`
	RedisPort     string `env:"REDIS_PORT" envDefault:"6379"`
	RedisPassword string `env:"REDIS_PASSWORD" envDefault:""`
	RedisDB       int    `env:"REDIS_DB" envDefault:"0"`

	// PostgreSQL
	DBHost     string `env:"DB_HOST" envDefault:"localhost"`
	DBPort     string `env:"DB_PORT" envDefault:"5432"`
	DBUser     string `env:"DB_USER" envDefault:"postgres"`
	DBPassword string `env:"DB_PASSWORD" envDefault:"postgres"`
	DBName     string `env:"DB_NAME" envDefault:"jobqueue"`
	DBSSLMode  string `env:"DB_SSLMODE" envDefault:"disable"`

	// Worker
	WorkerConcurrency int           `env:"WORKER_CONCURRENCY" envDefault:"10"`
	JobTimeout        time.Duration `env:"JOB_TIMEOUT" envDefault:"30s"`
}

func Load() (*Config, error) {
	if err := godotenv.Load(); err != nil {
		log.Println("⚠️ No .env file found, using system env vars")
	}

	cfg := &Config{}
	if err := env.Parse(cfg); err != nil {
		return nil, err
	}
	return cfg, nil
}
