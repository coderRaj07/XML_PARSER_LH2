# Quick Start

```bash
# Build and start all services
docker compose up --build -d

# Verify everything is healthy
docker compose ps

# Follow all worker logs
docker compose logs -f workflow-worker fetch-worker parse-worker summarize-worker
```

## Architecture

Each queue runs in its own OS process for true CPU parallelism:

```
workflow-worker (1 process per `--scale`)
fetch-worker    (N processes per `--scale`)
parse-worker    (M processes per `--scale`)
summarize-worker (K processes per `--scale`)
```

No thread pools — each process has its own event loop, so blocking CPU calls (`trafilatura.extract`, `generate_summary`) don't stall other queues.

## Scaling

Scale each queue independently based on its bottleneck:

```bash
# 3 fetch + 2 parse + 2 summarize = 7 worker containers
docker compose up -d --scale fetch-worker=3 --scale parse-worker=2 --scale summarize-worker=2
```

Each worker runs with `max_concurrent_activities=5` by default, tunable via `FETCH_WORKERS`, `PARSE_WORKERS`, `SUMMARIZE_WORKERS` env vars.

## Tuning per-queue concurrency

```bash
# At startup:
FETCH_WORKERS=10 PARSE_WORKERS=10 SUMMARIZE_WORKERS=10 docker compose up --build -d

# On an already running service (recreates that container):
docker compose up -d --scale fetch-worker=5 -e FETCH_WORKERS=10
```
