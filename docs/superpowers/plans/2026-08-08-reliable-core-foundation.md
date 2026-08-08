# Reliable Core Foundation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build ByteMQ's first Go foundation: module structure, core job state rules, retry/idempotency primitives, configuration validation, and a CLI mode skeleton.

**Architecture:** Start with pure domain and configuration code before persistence. The first implementation keeps production behavior explicit but avoids PostgreSQL code until the domain contracts are stable and test-covered.

**Tech Stack:** Go 1.26+, standard library only for this slice.

## Global Constraints

- Follow `rules.md` before changing code.
- ByteMQ is learning-first and production-shaped.
- ByteMQ starts as one Go binary with modes: `dev`, `server`, `scheduler`, and `worker`.
- PostgreSQL remains the v1 durable store, but this plan does not implement the database layer yet.
- ByteMQ provides at-least-once execution and must not claim exactly-once delivery.
- No Redis, NATS, Kubernetes, dashboard, workflows, or multi-region features in this slice.
- Use TDD: write each behavior test, watch it fail, then implement the minimal code.
- Use only ASCII text in source files.

---

## File Structure

Create this initial implementation structure:

```text
go.mod
cmd/bytemq/main.go
cmd/bytemq/main_test.go
internal/config/config.go
internal/config/config_test.go
internal/domain/errors.go
internal/domain/idempotency.go
internal/domain/idempotency_test.go
internal/domain/job.go
internal/domain/job_test.go
internal/domain/retry.go
internal/domain/retry_test.go
```

Responsibilities:

- `cmd/bytemq/main.go`: parse runtime mode and dispatch a startup message for now.
- `internal/config/config.go`: load and validate basic runtime configuration.
- `internal/domain/job.go`: define job states and allowed transitions.
- `internal/domain/retry.go`: define retry policy and backoff calculation.
- `internal/domain/idempotency.go`: validate idempotency key scope.
- `internal/domain/errors.go`: shared domain errors.

## Task 1: Go Module and Job State Machine

**Files:**
- Create: `go.mod`
- Create: `internal/domain/errors.go`
- Create: `internal/domain/job.go`
- Test: `internal/domain/job_test.go`

**Interfaces:**
- Produces: `type JobState string`
- Produces: constants `JobStateQueued`, `JobStateLeased`, `JobStateRunning`, `JobStateCompleted`, `JobStateFailed`, `JobStateRetryScheduled`, `JobStateDeadLettered`
- Produces: `func (s JobState) Terminal() bool`
- Produces: `func CanTransition(from JobState, to JobState) bool`
- Produces: `func TransitionJobState(from JobState, to JobState) error`
- Produces: `var ErrInvalidStateTransition error`

- [ ] **Step 1: Initialize `go.mod`**

```go
module github.com/sharvesh/bytemq

go 1.26
```

- [ ] **Step 2: Write failing job lifecycle tests**

```go
package domain

import "testing"

func TestCanTransitionAllowsCoreLifecycle(t *testing.T) {
	cases := []struct {
		name string
		from JobState
		to   JobState
	}{
		{"queued to leased", JobStateQueued, JobStateLeased},
		{"leased to running", JobStateLeased, JobStateRunning},
		{"running to completed", JobStateRunning, JobStateCompleted},
		{"running to failed", JobStateRunning, JobStateFailed},
		{"failed to retry scheduled", JobStateFailed, JobStateRetryScheduled},
		{"retry scheduled to queued", JobStateRetryScheduled, JobStateQueued},
		{"failed to dead lettered", JobStateFailed, JobStateDeadLettered},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if !CanTransition(tc.from, tc.to) {
				t.Fatalf("expected transition %s -> %s to be allowed", tc.from, tc.to)
			}
		})
	}
}

func TestCanTransitionRejectsUnsafeLifecycleChanges(t *testing.T) {
	cases := []struct {
		name string
		from JobState
		to   JobState
	}{
		{"completed to queued", JobStateCompleted, JobStateQueued},
		{"dead lettered to queued", JobStateDeadLettered, JobStateQueued},
		{"queued to completed", JobStateQueued, JobStateCompleted},
		{"leased to completed without running", JobStateLeased, JobStateCompleted},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if CanTransition(tc.from, tc.to) {
				t.Fatalf("expected transition %s -> %s to be rejected", tc.from, tc.to)
			}
		})
	}
}

func TestTransitionJobStateReturnsInvalidTransitionError(t *testing.T) {
	err := TransitionJobState(JobStateCompleted, JobStateQueued)
	if err == nil {
		t.Fatal("expected invalid transition error")
	}
	if err != ErrInvalidStateTransition {
		t.Fatalf("expected ErrInvalidStateTransition, got %v", err)
	}
}

func TestTerminalIdentifiesCompletedAndDeadLettered(t *testing.T) {
	if !JobStateCompleted.Terminal() {
		t.Fatal("completed should be terminal")
	}
	if !JobStateDeadLettered.Terminal() {
		t.Fatal("dead lettered should be terminal")
	}
	if JobStateQueued.Terminal() {
		t.Fatal("queued should not be terminal")
	}
}
```

- [ ] **Step 3: Run test to verify it fails**

Run: `go test ./internal/domain`

Expected: FAIL because `JobState`, state constants, and functions are undefined.

- [ ] **Step 4: Implement the minimal state machine**

Create `internal/domain/errors.go`:

```go
package domain

import "errors"

var ErrInvalidStateTransition = errors.New("invalid job state transition")
```

Create `internal/domain/job.go`:

```go
package domain

type JobState string

const (
	JobStateQueued         JobState = "queued"
	JobStateLeased         JobState = "leased"
	JobStateRunning        JobState = "running"
	JobStateCompleted      JobState = "completed"
	JobStateFailed         JobState = "failed"
	JobStateRetryScheduled JobState = "retry_scheduled"
	JobStateDeadLettered   JobState = "dead_lettered"
)

func (s JobState) Terminal() bool {
	return s == JobStateCompleted || s == JobStateDeadLettered
}

func CanTransition(from JobState, to JobState) bool {
	allowed := map[JobState][]JobState{
		JobStateQueued:         {JobStateLeased},
		JobStateLeased:         {JobStateRunning, JobStateQueued},
		JobStateRunning:        {JobStateCompleted, JobStateFailed},
		JobStateFailed:         {JobStateRetryScheduled, JobStateDeadLettered},
		JobStateRetryScheduled: {JobStateQueued},
	}

	for _, candidate := range allowed[from] {
		if candidate == to {
			return true
		}
	}
	return false
}

func TransitionJobState(from JobState, to JobState) error {
	if !CanTransition(from, to) {
		return ErrInvalidStateTransition
	}
	return nil
}
```

- [ ] **Step 5: Run test to verify it passes**

Run: `go test ./internal/domain`

Expected: PASS.

## Task 2: Retry Policy

**Files:**
- Create: `internal/domain/retry.go`
- Test: `internal/domain/retry_test.go`

**Interfaces:**
- Consumes: `internal/domain.ErrInvalidRetryPolicy`
- Produces: `type BackoffType string`
- Produces: constants `BackoffFixed`, `BackoffExponential`
- Produces: `type RetryPolicy struct { MaxAttempts int; Backoff BackoffType; InitialDelay time.Duration; MaxDelay time.Duration }`
- Produces: `func (p RetryPolicy) Validate() error`
- Produces: `func (p RetryPolicy) ShouldRetry(attempt int) bool`
- Produces: `func (p RetryPolicy) DelayForAttempt(attempt int) time.Duration`
- Produces: `var ErrInvalidRetryPolicy error`

- [ ] **Step 1: Write failing retry tests**

```go
package domain

import (
	"testing"
	"time"
)

func TestRetryPolicyShouldRetryWhileAttemptsRemain(t *testing.T) {
	policy := RetryPolicy{MaxAttempts: 3, Backoff: BackoffFixed, InitialDelay: time.Second}

	if !policy.ShouldRetry(1) {
		t.Fatal("attempt 1 should retry when max attempts is 3")
	}
	if !policy.ShouldRetry(2) {
		t.Fatal("attempt 2 should retry when max attempts is 3")
	}
	if policy.ShouldRetry(3) {
		t.Fatal("attempt 3 should not retry because max attempts is exhausted")
	}
}

func TestRetryPolicyFixedBackoffUsesInitialDelay(t *testing.T) {
	policy := RetryPolicy{MaxAttempts: 5, Backoff: BackoffFixed, InitialDelay: 5 * time.Second}

	if got := policy.DelayForAttempt(3); got != 5*time.Second {
		t.Fatalf("expected fixed delay 5s, got %s", got)
	}
}

func TestRetryPolicyExponentialBackoffDoublesByAttempt(t *testing.T) {
	policy := RetryPolicy{MaxAttempts: 5, Backoff: BackoffExponential, InitialDelay: time.Second}

	cases := []struct {
		attempt int
		want    time.Duration
	}{
		{1, time.Second},
		{2, 2 * time.Second},
		{3, 4 * time.Second},
	}

	for _, tc := range cases {
		if got := policy.DelayForAttempt(tc.attempt); got != tc.want {
			t.Fatalf("attempt %d: expected %s, got %s", tc.attempt, tc.want, got)
		}
	}
}

func TestRetryPolicyExponentialBackoffRespectsMaxDelay(t *testing.T) {
	policy := RetryPolicy{
		MaxAttempts:  10,
		Backoff:      BackoffExponential,
		InitialDelay: time.Second,
		MaxDelay:     3 * time.Second,
	}

	if got := policy.DelayForAttempt(5); got != 3*time.Second {
		t.Fatalf("expected capped delay 3s, got %s", got)
	}
}

func TestRetryPolicyValidateRejectsInvalidValues(t *testing.T) {
	cases := []RetryPolicy{
		{MaxAttempts: 0, Backoff: BackoffFixed, InitialDelay: time.Second},
		{MaxAttempts: 3, Backoff: "linear", InitialDelay: time.Second},
		{MaxAttempts: 3, Backoff: BackoffFixed, InitialDelay: 0},
	}

	for _, policy := range cases {
		if err := policy.Validate(); err != ErrInvalidRetryPolicy {
			t.Fatalf("expected ErrInvalidRetryPolicy for %+v, got %v", policy, err)
		}
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/domain`

Expected: FAIL because retry types and functions are undefined.

- [ ] **Step 3: Implement retry policy**

Create `internal/domain/retry.go`:

```go
package domain

import "time"

type BackoffType string

const (
	BackoffFixed       BackoffType = "fixed"
	BackoffExponential BackoffType = "exponential"
)

type RetryPolicy struct {
	MaxAttempts  int
	Backoff      BackoffType
	InitialDelay time.Duration
	MaxDelay     time.Duration
}

func (p RetryPolicy) Validate() error {
	if p.MaxAttempts < 1 {
		return ErrInvalidRetryPolicy
	}
	if p.InitialDelay <= 0 {
		return ErrInvalidRetryPolicy
	}
	if p.Backoff != BackoffFixed && p.Backoff != BackoffExponential {
		return ErrInvalidRetryPolicy
	}
	return nil
}

func (p RetryPolicy) ShouldRetry(attempt int) bool {
	return attempt < p.MaxAttempts
}

func (p RetryPolicy) DelayForAttempt(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}

	delay := p.InitialDelay
	if p.Backoff == BackoffExponential {
		for i := 1; i < attempt; i++ {
			delay *= 2
		}
	}

	if p.MaxDelay > 0 && delay > p.MaxDelay {
		return p.MaxDelay
	}
	return delay
}
```

Add to `internal/domain/errors.go`:

```go
var ErrInvalidRetryPolicy = errors.New("invalid retry policy")
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./internal/domain`

Expected: PASS.

## Task 3: Idempotency Scope

**Files:**
- Create: `internal/domain/idempotency.go`
- Test: `internal/domain/idempotency_test.go`

**Interfaces:**
- Produces: `type IdempotencyScope struct { Queue string; Key string }`
- Produces: `func NewIdempotencyScope(queue string, key string) (IdempotencyScope, error)`
- Produces: `func (s IdempotencyScope) Empty() bool`
- Produces: `var ErrInvalidIdempotencyScope error`

- [ ] **Step 1: Write failing idempotency tests**

```go
package domain

import "testing"

func TestNewIdempotencyScopeTrimsQueueAndKey(t *testing.T) {
	scope, err := NewIdempotencyScope(" payments ", " invoice-123 ")
	if err != nil {
		t.Fatalf("expected valid scope, got %v", err)
	}
	if scope.Queue != "payments" {
		t.Fatalf("expected trimmed queue, got %q", scope.Queue)
	}
	if scope.Key != "invoice-123" {
		t.Fatalf("expected trimmed key, got %q", scope.Key)
	}
}

func TestNewIdempotencyScopeAllowsEmptyKey(t *testing.T) {
	scope, err := NewIdempotencyScope("reports", "")
	if err != nil {
		t.Fatalf("expected empty idempotency key to be allowed, got %v", err)
	}
	if !scope.Empty() {
		t.Fatal("scope with no key should be empty")
	}
}

func TestNewIdempotencyScopeRejectsKeyWithoutQueue(t *testing.T) {
	_, err := NewIdempotencyScope("", "invoice-123")
	if err != ErrInvalidIdempotencyScope {
		t.Fatalf("expected ErrInvalidIdempotencyScope, got %v", err)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/domain`

Expected: FAIL because idempotency types and functions are undefined.

- [ ] **Step 3: Implement idempotency scope**

Create `internal/domain/idempotency.go`:

```go
package domain

import "strings"

type IdempotencyScope struct {
	Queue string
	Key   string
}

func NewIdempotencyScope(queue string, key string) (IdempotencyScope, error) {
	scope := IdempotencyScope{
		Queue: strings.TrimSpace(queue),
		Key:   strings.TrimSpace(key),
	}

	if scope.Key != "" && scope.Queue == "" {
		return IdempotencyScope{}, ErrInvalidIdempotencyScope
	}
	return scope, nil
}

func (s IdempotencyScope) Empty() bool {
	return s.Key == ""
}
```

Add to `internal/domain/errors.go`:

```go
var ErrInvalidIdempotencyScope = errors.New("invalid idempotency scope")
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./internal/domain`

Expected: PASS.

## Task 4: Configuration Validation

**Files:**
- Create: `internal/config/config.go`
- Test: `internal/config/config_test.go`

**Interfaces:**
- Produces: `type RuntimeMode string`
- Produces: constants `ModeDev`, `ModeServer`, `ModeScheduler`, `ModeWorker`
- Produces: `type Config struct { Mode RuntimeMode; DatabaseURL string; HTTPAddr string; WorkerID string; LeaseDuration time.Duration; HeartbeatInterval time.Duration; PollInterval time.Duration; LogLevel string }`
- Produces: `func Default(mode RuntimeMode) Config`
- Produces: `func (c Config) Validate() error`
- Produces: `var ErrInvalidConfig error`

- [ ] **Step 1: Write failing config tests**

```go
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/config`

Expected: FAIL because config types and functions are undefined.

- [ ] **Step 3: Implement config validation**

Create `internal/config/config.go`:

```go
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
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./internal/config`

Expected: PASS.

## Task 5: CLI Mode Skeleton

**Files:**
- Create: `cmd/bytemq/main.go`
- Test: `cmd/bytemq/main_test.go`

**Interfaces:**
- Consumes: `internal/config.RuntimeMode`
- Produces: `func run(args []string, getenv func(string) string, stdout io.Writer, stderr io.Writer) int`
- Produces: a binary entrypoint accepting `dev`, `server`, `scheduler`, and `worker`

- [ ] **Step 1: Write failing CLI tests**

```go
package main

import (
	"bytes"
	"strings"
	"testing"
)

func TestRunStartsInRequestedMode(t *testing.T) {
	var stdout bytes.Buffer
	var stderr bytes.Buffer

	exitCode := run(
		[]string{"bytemq", "scheduler"},
		func(key string) string {
			if key == "BYTEMQ_DATABASE_URL" {
				return "postgres://localhost/bytemq"
			}
			return ""
		},
		&stdout,
		&stderr,
	)

	if exitCode != 0 {
		t.Fatalf("expected exit code 0, got %d with stderr %q", exitCode, stderr.String())
	}
	if got := stdout.String(); strings.TrimSpace(got) != "bytemq starting in scheduler mode" {
		t.Fatalf("unexpected stdout %q", got)
	}
}

func TestRunRejectsInvalidConfiguration(t *testing.T) {
	var stdout bytes.Buffer
	var stderr bytes.Buffer

	exitCode := run([]string{"bytemq", "worker"}, func(string) string { return "" }, &stdout, &stderr)

	if exitCode != 1 {
		t.Fatalf("expected exit code 1, got %d", exitCode)
	}
	if !strings.Contains(stderr.String(), "invalid configuration") {
		t.Fatalf("expected invalid configuration error, got %q", stderr.String())
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./cmd/bytemq`

Expected: FAIL because `run` is undefined.

- [ ] **Step 3: Implement minimal CLI entrypoint**

Create `cmd/bytemq/main.go`:

```go
package main

import (
	"fmt"
	"io"
	"os"

	"github.com/sharvesh/bytemq/internal/config"
)

func main() {
	os.Exit(run(os.Args, os.Getenv, os.Stdout, os.Stderr))
}

func run(args []string, getenv func(string) string, stdout io.Writer, stderr io.Writer) int {
	mode := config.ModeDev
	if len(args) > 1 {
		mode = config.RuntimeMode(args[1])
	}

	cfg := config.Default(mode)
	cfg.DatabaseURL = getenv("BYTEMQ_DATABASE_URL")
	cfg.WorkerID = getenv("BYTEMQ_WORKER_ID")

	if err := cfg.Validate(); err != nil {
		fmt.Fprintf(stderr, "invalid configuration for mode %q: %v\n", mode, err)
		return 1
	}

	fmt.Fprintf(stdout, "bytemq starting in %s mode\n", cfg.Mode)
	return 0
}
```

- [ ] **Step 4: Run CLI test to verify it passes**

Run: `go test ./cmd/bytemq`

Expected: PASS.

- [ ] **Step 5: Run full verification**

Run:

```bash
gofmt -w cmd internal
go test ./...
go run ./cmd/bytemq dev
```

Expected:

- `go test ./...` passes.
- `go run ./cmd/bytemq dev` exits with config error unless `BYTEMQ_DATABASE_URL` is set.

Run:

```bash
BYTEMQ_DATABASE_URL=postgres://localhost/bytemq BYTEMQ_WORKER_ID=worker-1 go run ./cmd/bytemq dev
```

Expected:

```text
bytemq starting in dev mode
```

## Self-Review Checklist

- Job state transitions match `architecture.md` and `concept-docs/job-lifecycle.md`.
- Retry policy supports fixed and exponential backoff only.
- Idempotency scope is queue plus key.
- Config exposes lease duration, heartbeat interval, poll interval, database URL, and runtime mode.
- CLI supports the agreed single-binary mode shape.
- No PostgreSQL implementation is included in this slice.
- No exactly-once claim is introduced.
- All new behavior is covered by tests except the minimal CLI print path.
