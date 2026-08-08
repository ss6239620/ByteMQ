# Scheduling and Backpressure

Scheduling decides when a job should run. Backpressure decides what the system
should do when producers are faster than workers.

## V1 Scheduling

ByteMQ v1 should keep scheduling simple.

The scheduler should select jobs that are:

- Not terminal.
- Not currently leased.
- Due to run now.
- Within retry policy.

The first scheduling strategy can be FIFO within a queue, using creation time or
run time ordering.

## Why Not Start With Adaptive Scheduling?

Adaptive scheduling is a strong future differentiator, but it depends on a
correct reliable core.

Before adaptive scheduling, ByteMQ must prove:

- Jobs are durable.
- Leases work.
- Workers heartbeat.
- Expired leases recover.
- Retries are correct.
- Dead-lettering is reliable.

Advanced scheduling on top of a weak lifecycle creates confusing behavior.

## Future Scheduling Strategies

After the reliable core, ByteMQ can add:

- Priority scheduling.
- Deadline-first scheduling.
- Shortest-job-first scheduling.
- Weighted fair queueing.
- Tenant fairness.
- Resource-aware scheduling.
- Cost-aware scheduling.
- Adaptive scheduling.

These should be added one at a time with tests and clear operational behavior.

## Explainable Scheduling

ByteMQ should eventually explain why a job is waiting.

Examples:

```text
No worker has the required capability.
The job is waiting for retry backoff.
The queue is paused.
The tenant concurrency limit is reached.
The job's run_after time has not arrived.
```

This requires scheduler decisions to be represented as data, not only logs.

## Worker Capacity

Early workers can advertise simple capacity:

```text
worker_id
max_concurrency
active_jobs
heartbeat_time
```

Future workers may advertise:

- Supported job types.
- CPU capacity.
- Memory capacity.
- GPU availability.
- Current load.
- Region or zone.

Do not implement advanced worker capability routing in v1. Preserve a place for
it in the architecture.

## Backpressure Problem

Backpressure appears when enqueue rate exceeds processing rate.

Example:

```text
producers: 100,000 jobs/sec
workers:    10,000 jobs/sec
```

Without backpressure, queues grow without bound. Storage, memory, and downstream
systems can fail.

## Future Backpressure Signals

ByteMQ should eventually measure:

- Queue depth.
- Queue age.
- Ingestion rate.
- Processing rate.
- Retry rate.
- Estimated drain time.
- Worker capacity.
- Dead-letter rate.

These signals tell operators whether the system is healthy.

## Future Backpressure Controls

Possible controls:

- Reject enqueue after queue limit.
- Throttle producers.
- Apply per-tenant quotas.
- Apply per-queue rate limits.
- Shed low-priority work.
- Pause retry storms.
- Emit autoscaling signals.

Backpressure controls should be explicit. Silent dropping is not acceptable.

## V1 Preparation

Even without full backpressure, v1 should store enough data to calculate:

- Number of queued jobs.
- Oldest queued job age.
- Number of leased jobs.
- Number of failed attempts.
- Number of retry-scheduled jobs.
- Number of dead-lettered jobs.

These metrics will support future scheduling and operations work.

