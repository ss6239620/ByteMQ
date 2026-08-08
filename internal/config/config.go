package config

import (
	"errors"
	"strings"
	"time"
)

var ErrInvalidConfig = errors.New("invalid config")

type RuntimeMode string

const (
	ModeDev       RuntimeMode = "dev"
	ModeServer    RuntimeMode = "server"
	ModeScheduler RuntimeMode = "scheduler"
	ModeWorker    RuntimeMode = "worker"
)

type Config struct {
	Mode              RuntimeMode
	DatabaseURL       string
	HTTPAddr          string
	WorkerID          string
	LeaseDuration     time.Duration
	HeartbeatInterval time.Duration
	PollInterval      time.Duration
	LogLevel          string
}

func Default(mode RuntimeMode) Config {
	return Config{
		Mode:              mode,
		HTTPAddr:          ":8080",
		LeaseDuration:     30 * time.Second,
		HeartbeatInterval: 10 * time.Second,
		PollInterval:      time.Second,
		LogLevel:          "info",
	}
}

func (c Config) Validate() error {
	switch c.Mode {
	case ModeDev, ModeServer, ModeScheduler, ModeWorker:
	default:
		return ErrInvalidConfig
	}

	if strings.TrimSpace(c.DatabaseURL) == "" {
		return ErrInvalidConfig
	}
	if c.LeaseDuration <= 0 || c.HeartbeatInterval <= 0 || c.PollInterval <= 0 {
		return ErrInvalidConfig
	}
	if c.HeartbeatInterval >= c.LeaseDuration {
		return ErrInvalidConfig
	}
	if c.Mode == ModeWorker && strings.TrimSpace(c.WorkerID) == "" {
		return ErrInvalidConfig
	}
	return nil
}
