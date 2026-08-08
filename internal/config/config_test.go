package config

import (
	"testing"
	"time"
)

func TestDefaultConfigUsesRequestedModeAndSafeDurations(t *testing.T) {
	cfg := Default(ModeDev)

	if cfg.Mode != ModeDev {
		t.Fatalf("expected mode dev, got %s", cfg.Mode)
	}
	if cfg.LeaseDuration != 30*time.Second {
		t.Fatalf("expected lease duration 30s, got %s", cfg.LeaseDuration)
	}
	if cfg.HeartbeatInterval != 10*time.Second {
		t.Fatalf("expected heartbeat interval 10s, got %s", cfg.HeartbeatInterval)
	}
	if cfg.PollInterval != time.Second {
		t.Fatalf("expected poll interval 1s, got %s", cfg.PollInterval)
	}
}

func TestConfigValidateAcceptsKnownModes(t *testing.T) {
	for _, mode := range []RuntimeMode{ModeDev, ModeServer, ModeScheduler, ModeWorker} {
		cfg := Default(mode)
		cfg.DatabaseURL = "postgres://localhost/bytemq"
		cfg.WorkerID = "worker-1"

		if err := cfg.Validate(); err != nil {
			t.Fatalf("expected mode %s to validate, got %v", mode, err)
		}
	}
}

func TestConfigValidateRejectsInvalidMode(t *testing.T) {
	cfg := Default(RuntimeMode("unknown"))
	cfg.DatabaseURL = "postgres://localhost/bytemq"

	if err := cfg.Validate(); err != ErrInvalidConfig {
		t.Fatalf("expected ErrInvalidConfig, got %v", err)
	}
}

func TestConfigValidateRequiresDatabaseURL(t *testing.T) {
	cfg := Default(ModeServer)

	if err := cfg.Validate(); err != ErrInvalidConfig {
		t.Fatalf("expected ErrInvalidConfig, got %v", err)
	}
}

func TestConfigValidateRequiresHeartbeatShorterThanLease(t *testing.T) {
	cfg := Default(ModeWorker)
	cfg.DatabaseURL = "postgres://localhost/bytemq"
	cfg.WorkerID = "worker-1"
	cfg.HeartbeatInterval = cfg.LeaseDuration

	if err := cfg.Validate(); err != ErrInvalidConfig {
		t.Fatalf("expected ErrInvalidConfig, got %v", err)
	}
}
