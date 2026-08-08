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
