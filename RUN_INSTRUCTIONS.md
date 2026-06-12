# Quick Start

```bash
# Build and start all services (1 worker pod is enough for most cases)
docker compose up --build -d

# Verify everything is healthy
docker compose ps

# Follow worker logs
docker compose logs -f worker
```

## Scaling

A single worker pod runs **16 internal workers** by default (1 workflow + 5 fetch + 5 parse + 5 summarize) — enough for most workloads.

Only scale horizontally if you need more CPU/memory:

```bash
# Scale to 5 Docker containers (80 workers total)
docker compose up -d --scale worker=5
```

## Tuning per-queue workers

Adjust workers per stage without adding containers:

```bash
# At startup:
FETCH_WORKERS=10 PARSE_WORKERS=10 SUMMARIZE_WORKERS=10 docker compose up --build -d

# Or on an already running container:
docker compose run -e FETCH_WORKERS=10 -e PARSE_WORKERS=10 -e SUMMARIZE_WORKERS=10 worker
```

## Architecture

```
1 pod = 1 workflow + 5 fetch + 5 parse + 5 summarize = 16 workers
```
