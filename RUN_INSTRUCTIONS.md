# 0. Stop all other existing docker containers
docker stop $(docker ps -q)

# 1. Build and start all services (1 worker pod by default)
docker compose up --build -d

# 2. Wait for everything to be healthy (~30s)
docker compose ps

# 3. Scale worker to 5 replicas
docker compose up -d --scale worker=5

# 4. Verify all 5 worker pods are running
docker compose ps worker

# 5. Monitor worker logs (all 5)
docker compose logs -f worker

---

## Architecture Note

Each worker pod runs 16 Temporal workers:
- 1 workflow worker (`xml-feed-workflow-queue`)
- 5 fetch workers (`xml-feed-fetch-queue`)
- 5 parse workers (`xml-feed-parse-queue`)
- 5 summarize workers (`xml-feed-summarize-queue`)

With 5 replicas, that's 80 concurrent workers across all pods.
