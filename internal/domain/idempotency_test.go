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
