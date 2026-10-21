# Distributed Job Processing Platform

An asynchronous job-processing service built with Go, PostgreSQL, Apache Kafka,
Redis, Docker Compose, and Prometheus instrumentation.

## Current architecture

```text
Client
  |
  v
Go HTTP API
  |-- PostgreSQL: durable job records and idempotency
  |-- Kafka: publish job IDs to the jobs topic
  `-- Prometheus: /metrics

Kafka jobs topic
  |
  v
Worker
  |-- claims pending jobs in PostgreSQL
  |-- processes jobs
  `-- updates status and retries failures

Redis client
  `-- available for status caching; GET currently reads PostgreSQL directly
```

PostgreSQL is the source of truth. Kafka distributes work, and the worker
updates the durable job state.

## Job lifecycle

```text
pending -> running -> completed
                   \
                    -> pending -> retry
                    \
                     -> failed
```

Jobs with `type: "fail"` intentionally simulate processing errors. Failed jobs
are retried until `max_attempts` is reached, then marked `failed`.

## Requirements

- Go 1.27 or newer
- Docker Desktop with Docker Compose

## Start infrastructure

From the repository root:

```powershell
docker compose up -d
```

Create the Kafka topic once:

```powershell
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --create `
  --topic jobs `
  --bootstrap-server localhost:9092 `
  --partitions 3 `
  --replication-factor 1
```

The command may report that the topic already exists; that is safe.

Apply migrations in order:

```powershell
Get-Content .\migrations\001_create_jobs.sql |
  docker compose exec -T postgres psql -U jobuser -d jobsdb

Get-Content .\migrations\002_add_job_attempts.sql |
  docker compose exec -T postgres psql -U jobuser -d jobsdb

Get-Content .\migrations\003_add_idempotency.sql |
  docker compose exec -T postgres psql -U jobuser -d jobsdb
```

## Run the application

Use separate terminals from the project root:

```powershell
go run .\cmd\api
```

```powershell
go run .\cmd\worker
```

The API listens on `http://localhost:8080`.

## API

### Submit a job

`Idempotency-Key` is required.

```powershell
$body = @{
    type = "email"
    payload = "send welcome email"
} | ConvertTo-Json

Invoke-RestMethod `
  -Uri http://localhost:8080/jobs `
  -Method POST `
  -Headers @{"Idempotency-Key"="example-001"} `
  -ContentType "application/json" `
  -Body $body
```

The response contains the job ID and starts in `pending`:

```json
{
  "id": "job-uuid",
  "message": "job received",
  "type": "email",
  "status": "pending"
}
```

Submitting the same request with the same idempotency key returns the original
job ID instead of creating a second job.

### Retrieve a job

```powershell
Invoke-RestMethod `
  -Uri http://localhost:8080/jobs/job-uuid `
  -Method GET
```

### Prometheus metrics

```powershell
Invoke-WebRequest http://localhost:8080/metrics -UseBasicParsing
```

Custom counters include:

- `jobs_submitted_total`
- `jobs_completed_total`
- `jobs_failed_total`

## Tests and formatting

```powershell
gofmt -w .\cmd\api\main.go
gofmt -w .\cmd\worker\main.go
gofmt -w (Get-ChildItem .\internal\*\*.go).FullName
go test ./...
```

The repository includes validation unit tests and a PostgreSQL-backed
idempotency integration test.

## Project layout

```text
cmd/
  api/                  HTTP API entry point
  worker/               Kafka consumer and job processor
internal/
  cache/                Redis client
  database/             PostgreSQL connection pool
  jobs/                 Models, handlers, and repository queries
  messaging/            Kafka publisher
  metrics/              Prometheus counters
migrations/             PostgreSQL schema migrations
docker-compose.yml      Local Kafka, PostgreSQL, and Redis
```

## Known limitations

- Kafka, PostgreSQL, and Redis are configured as single local development
  instances.
- Job processing is simulated; no real email or external job executor exists.
- Redis is not yet used in the `GET /jobs/{id}` handler.
- Authentication, rate limiting, TLS, Kubernetes deployment, and Grafana
  dashboards are outside the current v1 scope.
