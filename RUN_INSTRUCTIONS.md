# 1. Build and start all services (1 worker by default)
docker compose up --build -d

# 2. Wait for everything to be healthy (~30s)
docker compose ps

# 3. Scale worker to 5 replicas
docker compose up -d --scale worker=5

# 4. Verify all 5 workers are running
docker compose ps worker

# 5. Monitor worker logs (all 5)
docker compose logs -f worker