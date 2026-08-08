# Concept Docs

This folder explains ByteMQ's core distributed-systems concepts. Read these
documents to understand how the system works before changing implementation
code.

ByteMQ starts with a reliable PostgreSQL-backed job runtime and grows in phases.
The first goal is not maximum features. The first goal is correct behavior under
common production failures.

## Concept Map

- `job-lifecycle.md`: how jobs move through states from enqueue to completion,
  retry, or dead-letter.
- `leases-and-heartbeats.md`: how workers temporarily own jobs and how ByteMQ
  recovers from worker crashes.
- `retries-idempotency.md`: how ByteMQ handles failures without pretending to
  provide exactly-once execution.
- `scheduling-and-backpressure.md`: how job selection starts simple and grows
  toward fair, explainable, capacity-aware scheduling.
- `observability-and-operations.md`: how operators understand what happened and
  what will happen next.

## The Big Idea

A production job queue is not only a list of tasks. It is a state machine plus a
recovery protocol.

ByteMQ must know:

- Which jobs exist?
- Which jobs are waiting?
- Which jobs are currently leased?
- Which worker owns each lease?
- When does ownership expire?
- Which jobs failed?
- Which jobs should retry?
- Which jobs are permanently dead-lettered?
- Why is a job waiting?
- What should an operator do next?

This is why the project starts with durable state, leases, heartbeats, retries,
and observability before advanced scheduling or a dashboard.

## Core Promise

ByteMQ's honest promise:

```text
If enqueue succeeds, ByteMQ should not lose the job. It will execute the job at
least once, may execute it more than once after failures, and will expose enough
state to understand retries, failures, and recovery.
```

This is the foundation for every future feature.

