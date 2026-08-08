# Retries and Idempotency

Retries make a job runtime useful. Idempotency makes retries safe.

## At-Least-Once Execution

ByteMQ is an at-least-once system.

That means:

- If enqueue succeeds, ByteMQ should not lose the job.
- ByteMQ will try to execute the job.
- ByteMQ may execute the same job more than once after failures.

This is the correct promise for a practical job runtime.

## Why Exactly-Once Is Not the Promise

Consider a payment job:

```text
worker charges customer
payment succeeds
worker loses network before reporting success
lease expires
another worker retries the job
customer may be charged again
```

ByteMQ cannot know whether the external payment succeeded unless the payment
system and user code cooperate. Claiming exactly-once would be dishonest.

Instead, ByteMQ should provide:

- Durable job state.
- Attempt tracking.
- Retry control.
- Idempotency keys on enqueue.
- Clear duplicate-aware documentation.

## Retry Policy

A retry policy should answer:

- How many attempts are allowed?
- How long should ByteMQ wait before retrying?
- Does delay grow after each failure?
- Is there a maximum delay?
- Should jitter be added later?

Early policy fields:

```text
max_attempts
backoff_type
initial_delay
max_delay
```

Supported v1 backoff can start simple:

- Fixed delay.
- Exponential delay.

Jitter can come later.

## Attempt Records

Each execution attempt should record:

- Attempt number.
- Worker ID.
- Lease ID.
- Started time.
- Finished time.
- Success or failure.
- Error message on failure.
- Retry decision.

Attempts make job history explainable.

## Failure Categories

Failure classification can be simple in v1 and smarter later.

Future categories:

- Transient.
- Permanent.
- Rate limited.
- Timeout.
- Dependency failure.
- Authentication failure.
- Resource exhaustion.
- Unknown.

The initial system can store raw errors first. Classification can be added once
the lifecycle is reliable.

## Idempotency Keys

An idempotency key lets a producer safely repeat an enqueue request.

Example:

```text
queue: payments
idempotency key: charge:invoice-123
```

If the producer retries the same enqueue request, ByteMQ should return the
existing job instead of creating unlimited duplicates.

The uniqueness scope should likely include:

- Queue.
- Idempotency key.

Tenant or project should join that scope once multi-tenancy exists.

## Handler Idempotency

Enqueue idempotency does not solve every duplicate problem.

Handlers that perform side effects must also be idempotent. Examples:

- Payment providers should receive their own idempotency key.
- Email jobs should record whether a message ID was already sent.
- Report generation should write to deterministic output paths.
- Webhook delivery should record delivery attempts and response IDs.

ByteMQ should make this clear in SDKs and docs.

## Dead-Lettering

A job moves to dead-letter when it should stop retrying.

Reasons:

- Attempts exhausted.
- Permanent failure policy.
- Expired job.
- Manual cancellation later.

Dead-letter records should preserve debugging context. Operators should be able
to inspect the job and decide whether to requeue manually in a future version.

## Key Tests

Retry and idempotency tests should prove:

- Failed jobs retry when attempts remain.
- Retry delay is calculated correctly.
- Exhausted jobs dead-letter.
- Idempotency keys return the existing job.
- Attempt history is preserved.
- A stale worker cannot fail or complete a job after lease loss.

