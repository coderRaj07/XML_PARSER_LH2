# Quick Start

```bash
# 1. Build and start all services (1 worker pod by default)
docker compose up --build -d

# 2. Wait for everything to be healthy (~30s)
docker compose ps

# 3. Scale worker pods
docker compose up -d --scale worker=5

# 4. Verify
docker compose ps worker

# 5. Monitor
docker compose logs -f worker
```

## Tuning per-queue workers

Each worker pod runs 16 workers by default (1 workflow + 5 fetch + 5 parse + 5 summarize). Override per-queue counts via env vars:

```bash
# At startup:
FETCH_WORKERS=10 PARSE_WORKERS=10 SUMMARIZE_WORKERS=10 docker compose up --build -d

# Or when scaling:
docker compose run -e FETCH_WORKERS=10 -e PARSE_WORKERS=10 -e SUMMARIZE_WORKERS=10 worker
```

## Architecture

```
1 worker pod = 1 workflow worker + 5 fetch + 5 parse + 5 summarize = 16 workers
5 worker pods × 16 workers = 80 concurrent workers total
```
