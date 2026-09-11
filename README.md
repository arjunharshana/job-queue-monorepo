# PostgreSQL Distributed Job Queue

A production-shaped, Postgres-first job queue built from scratch in TypeScript, a smaller, from-first-principles version of what BullMQ, Sidekiq, and Celery do. Built to demonstrate real backend engineering depth: concurrency-safe job claiming, transactional state transitions, retry/backoff design, and a tested, production-shaped architecture.

---

## Architecture

```text
job-queue-monorepo/
├── apps/
│   ├── api/       — HTTP API: enqueue jobs and check status
│   └── worker/    — Job-processing daemon: claims, runs, and reports jobs
└── packages/
    └── core/      — Shared queue engine; the only layer that talks to Postgres
```

The `core` package acts as a strict internal library. The API and worker are independent, separately deployable services that communicate exclusively through the durable PostgreSQL store.

### How it works

```text
                         ┌──────────────┐
                         │     API      │
                         │  HTTP :3000  │
                         └──────┬───────┘
                                │
                                ▼
                    ┌──────────────────────┐
                    │     PostgreSQL       │
                    │                      │
                    │  jobs + job_events  │
                    └──────▲───────────────┘
                           │
             ┌─────────────┼─────────────┐
             │             │             │
       ┌─────┴─────┐ ┌─────┴─────┐ ┌─────┴─────┐
       │  Worker 1 │ │  Worker 2 │ │  Worker 3 │
       └───────────┘ └───────────┘ └───────────┘
```

Workers independently poll PostgreSQL for available jobs. PostgreSQL provides the coordination mechanism, so no external broker or application-level distributed lock is required.

---

## Job Lifecycle

Jobs move through a small, explicit state machine:

```text
pending ──(claim)──> active ──(complete)──> completed
   ▲                    │                         [terminal]
   │                    │
   │              (failure / lease expiry)
   │                    │
   │             retries remaining
   │                    │
   └────────────────────┘
                        │
                 retries exhausted
                        ▼
                      dead
                  [terminal, manually
                    re-queueable]
```

### States

| State | Description |
|---|---|
| `pending` | Waiting to be claimed by a worker |
| `active` | Currently leased to a worker |
| `completed` | Successfully processed; terminal state |
| `dead` | Failed permanently after retries; terminal state |

---

## Key Design Decisions:

### Atomic concurrency via `SKIP LOCKED`

- PostgreSQL row-level locking ensures safe distribution. When multiple workers poll simultaneously, locked rows are instantly bypassed, eliminating race conditions without application-level coordination.

### Raw SQL over ORMs

- Hand-written SQL via `pg` keeps Postgres-specific mechanics (`SKIP LOCKED`, transactional state transitions, lease handling) explicit and visible rather than abstracted away.

### Lease-based zombie reaping

- Claimed jobs receive a time-bound lease instead of holding an active database lock. A background reaper sweeps expired leases (from crashed workers) back into the retry cycle for automatic self-healing.

### Exponential backoff with jitter

- Failed jobs retry with a doubling base delay plus up to 20% random jitter to prevent downstream service flooding.

### At-least-once delivery

- Handlers must be idempotent. If a worker completes a task but crashes before database acknowledgment, the lease expires and the job will deliberately be reprocessed.

### Immutable audit trail

- Every state change appends a record to `job_events` within the same transaction, providing a complete, historically accurate log for debugging and operational visibility.

---

## Tech Stack

| Layer | Technology |
|---|---|
| Language | TypeScript (Strict Mode) |
| Runtime | Node.js 22 |
| Database | PostgreSQL 16 |
| API | Express + Zod |
| Database Access | `pg` (Raw SQL) |
| Migrations | `node-pg-migrate` |
| Testing | Vitest |
| Deployment | Docker + Docker Compose |
| Architecture | API + independently scalable worker replicas |

---

## Quick Start

The easiest way to run the complete distributed stack is with Docker Compose.

### Prerequisites

- Docker
- Docker Compose
- Node.js 22
- npm

### 1. Boot the database

```bash
docker compose up -d postgres
```

### 2. Apply database migrations

Run the migration command from the host:

```bash
DATABASE_URL=postgres://postgres:password@localhost:5432/job_queue npm run migrate up
```

### 3. Boot the cluster

```bash
docker compose up -d --build
```

This starts:

- API
- Worker 1
- Worker 2
- Worker 3

### 3. Stream the logs

```bash
docker compose logs -f
```

With multiple workers running, the logs can be used to observe jobs being distributed across worker replicas.

---

## API

The API exposes endpoints for creating jobs and checking their status.

### Enqueue a job

```bash
curl -X POST http://localhost:3000/jobs \
  -H "Content-Type: application/json" \
  -d '{
    "queueName": "email_notifications",
    "payload": {
      "userId": "123",
      "template": "welcome"
    }
  }'
```

The API validates the request using Zod and persists the job to PostgreSQL.

### Check job status

```bash
curl http://localhost:3000/jobs/<job-id>
```

The response can be used to inspect the current state of the job and its processing metadata.

---

## Testing

The test suite runs against an isolated PostgreSQL database:

```text
job_queue_test
```

Tests cover the complete lifecycle and important distributed-system guarantees.

Run the automated suite with:

```bash
npm run test:run
```

### What is tested

The test suite verifies behavior including:

- job creation
- job claiming
- successful completion
- failure handling
- retries
- exponential backoff
- retry exhaustion
- dead-lettering
- lease expiration
- worker recovery
- concurrent job claiming
- audit event creation
- prevention of duplicate claims

---

## Manual Verification

In addition to automated tests, the repository includes standalone scripts designed to make the system's behavior easy to observe.

### Concurrency test

```bash
npx tsx scripts/manual-concurrency-test.ts
```

Demonstrates that multiple workers competing for jobs do not claim the same job because of PostgreSQL's `SKIP LOCKED` behavior.

### Failure and retry cycle

```bash
npx tsx scripts/failure-cycle-test.ts
```

This makes the exponential backoff and dead-letter behavior visible outside the test runner.

### Worker crash / lease recovery

```bash
npx tsx scripts/manual-reaper-test.ts
```

Demonstrates that an active job whose worker disappears can be detected after its lease expires and returned to the appropriate retry/dead-letter path.

---