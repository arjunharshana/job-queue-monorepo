# PostgreSQL Distributed Job Queue

A production-shaped, Postgres-first job queue built from scratch in TypeScript, a smaller, from-first-principles version of what BullMQ, Sidekiq, and Celery do. Built to demonstrate real backend engineering depth: concurrency-safe job claiming, transactional state transitions, retry/backoff design, and a tested, production-shaped architecture.

---

## Architecture

```text
job-queue-monorepo/
├── apps/
│   ├── api/       — HTTP API: enqueue jobs (buffered via Redis), check status, WebSocket broadcast
│   └── worker/    — Job-processing daemon: claims, runs, and reports jobs
└── packages/
    └── core/      — Shared queue engine; the only layer that talks to Postgres
```

The `core` package acts as a strict internal library. The API and worker are independent, separately deployable services that communicate exclusively through the durable PostgreSQL store.

### How it works

```text
   ┌──────────────┐        ┌───────────┐        ┌─────────────┐
   │   Client     │─POST─> │    API    │─RPUSH->│    Redis    │
   └──────────────┘        │ HTTP :3000│        │ (buffer +   │
          ▲                └─────┬─────┘        │  pub/sub)   │
          │                      │              └─────────────┘
          │ WebSocket            │ SUBSCRIBE            │ BLPOP
          │ (live events)        │ job_events:broadcast │
          │                      ▼                      ▼
          │               ┌──────────────┐        ┌─────────────┐
          └────────────── │   Dashboard  │        │   Syncer    │
                          │   (planned)  │        │  (batches   │
                          └──────────────┘        │  into PG)   │
                                                  └──────┬──────┘
                                                          │
                                                          ▼
                                            ┌──────────────────────┐
                                            │     PostgreSQL       │
                                            │  jobs + job_events   │
                                            └──────▲───────────────┘
                                                   │
                                     ┌─────────────┼─────────────┐
                                     │             │             │
                               ┌─────┴─────┐ ┌─────┴─────┐ ┌─────┴─────┐
                               │  Worker 1 │ │  Worker 2 │ │  Worker 3 │
                               └───────────┘ └───────────┘ └───────────┘
```

The API buffers incoming jobs in Redis and returns immediately; a separate syncer batches them into PostgreSQL. Workers still coordinate exclusively through PostgreSQL — Redis is used only for ingestion buffering and live event broadcast, not for job claiming.

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

### Redis-buffered ingestion, eventual consistency

- `POST /jobs` writes to a Redis list and returns `202 Accepted` immediately, rather than blocking on a Postgres transaction. A separate syncer process batches buffered jobs into PostgreSQL.

### Live updates via Redis Pub/Sub + WebSockets

- Workers publish a lightweight event to a Redis channel on every state transition (claimed, completed, failed, reaped). The API subscribes and fans events out to connected WebSocket clients. A dropped event affects only the live view, never the durable state in `job_events`.

---

## Tech Stack

| Layer | Technology |
|---|---|
| Language | TypeScript (Strict Mode) |
| Runtime | Node.js 22 |
| Database | PostgreSQL 16 |
| Message broker | Redis (ingestion buffer + pub/sub) |
| API | Express + Zod |
| Real-time transport | `ws` (raw WebSocket, no framework) |
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

### 1. Boot the database and message broker

```bash
docker compose up -d postgres redis
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
- Syncer
- Worker 1
- Worker 2
- Worker 3

### 4. Stream the logs

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

The API validates the request using Zod, buffers it in Redis, and returns immediately with a `202` and the job's ID. The job becomes visible via `GET /jobs/:id` once the syncer has flushed it to PostgreSQL.

### Check job status

```bash
curl http://localhost:3000/jobs/<job-id>
```

The response can be used to inspect the current state of the job and its processing metadata.

### Live job events

Connect a WebSocket client to the API's HTTP port to receive job state changes as they happen:

```bash
npx tsx scripts/websocket-listener-test.ts
```

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
- batch enqueueing

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

### Eventual consistency timing

```bash
npx tsx scripts/consistency-test.ts
```

Measures the actual gap between a job being accepted (`202`) and appearing in PostgreSQL, making the Redis-buffered ingestion tradeoff concrete rather than theoretical.

### Live event broadcast

```bash
npx tsx scripts/websocket-listener-test.ts
```

Connects to the API's WebSocket endpoint and prints job state-change events as workers process jobs — direct evidence the Redis pub/sub → WebSocket bridge works end to end.

---