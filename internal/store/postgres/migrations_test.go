package postgres

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

func newTestPool(t *testing.T) *pgxpool.Pool {
	t.Helper()

	databaseURL := os.Getenv("BYTEMQ_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("set BYTEMQ_TEST_DATABASE_URL to run PostgreSQL integration tests")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	adminPool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		t.Fatalf("connect admin pool: %v", err)
	}
	schema := fmt.Sprintf("bytemq_test_%d", time.Now().UnixNano())
	if _, err := adminPool.Exec(ctx, `CREATE SCHEMA `+schema); err != nil {
		adminPool.Close()
		t.Fatalf("create test schema: %v", err)
	}

	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		_, _ = adminPool.Exec(cleanupCtx, `DROP SCHEMA IF EXISTS `+schema+` CASCADE`)
		adminPool.Close()
	})

	cfg, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		t.Fatalf("parse pool config: %v", err)
	}
	cfg.ConnConfig.RuntimeParams["search_path"] = schema

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatalf("connect schema pool: %v", err)
	}
	t.Cleanup(pool.Close)

	return pool
}

func TestMigrateCreatesInitialTables(t *testing.T) {
	ctx := context.Background()
	pool := newTestPool(t)

	if err := Migrate(ctx, pool); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	for _, table := range []string{"schema_migrations", "jobs", "job_attempts", "job_events"} {
		t.Run(table, func(t *testing.T) {
			var exists bool
			err := pool.QueryRow(ctx, `
				SELECT EXISTS (
					SELECT 1
					FROM information_schema.tables
					WHERE table_schema = current_schema()
					AND table_name = $1
				)
			`, table).Scan(&exists)
			if err != nil {
				t.Fatalf("query table existence: %v", err)
			}
			if !exists {
				t.Fatalf("expected table %s to exist", table)
			}
		})
	}
}

func TestMigrateRecordsVersionOnce(t *testing.T) {
	ctx := context.Background()
	pool := newTestPool(t)

	if err := Migrate(ctx, pool); err != nil {
		t.Fatalf("first migrate: %v", err)
	}
	if err := Migrate(ctx, pool); err != nil {
		t.Fatalf("second migrate: %v", err)
	}

	var count int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM schema_migrations WHERE version = $1`, "000001_init").Scan(&count); err != nil {
		t.Fatalf("count migration rows: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected migration version recorded once, got %d", count)
	}
}

func TestTestPoolUsesTemporarySchema(t *testing.T) {
	pool := newTestPool(t)

	var schema string
	if err := pool.QueryRow(context.Background(), `SELECT current_schema()`).Scan(&schema); err != nil {
		t.Fatalf("query current schema: %v", err)
	}
	if !strings.HasPrefix(schema, "bytemq_test_") {
		t.Fatalf("expected temporary test schema, got %q", schema)
	}
}
