import os
from datetime import timedelta

from temporalio.common import RetryPolicy

WORKFLOW_NAME = "job-workflow"
ENRICHMENT_WORKFLOW_NAME = "enrichment-workflow"

WORKFLOW_QUEUE = os.getenv("TEMPORAL_WORKFLOW_QUEUE", "xml-feed-workflow-queue")
URL_WORKFLOW_QUEUE = os.getenv("TEMPORAL_URL_WORKFLOW_QUEUE", "xml-feed-url-workflow-queue")
FETCH_QUEUE = os.getenv("TEMPORAL_FETCH_QUEUE", "xml-feed-fetch-queue")
PARSE_QUEUE = os.getenv("TEMPORAL_PARSE_QUEUE", "xml-feed-parse-queue")
ENRICHMENT_QUEUE = os.getenv("TEMPORAL_ENRICHMENT_QUEUE", "xml-feed-enrichment-queue")
SUMMARIZE_QUEUE = os.getenv("TEMPORAL_SUMMARIZE_QUEUE", "xml-feed-summarize-queue")

FETCH_WORKER_COUNT = int(os.getenv("FETCH_WORKERS", "5"))
PARSE_WORKER_COUNT = int(os.getenv("PARSE_WORKERS", "5"))
ENRICHMENT_WORKER_COUNT = int(os.getenv("ENRICHMENT_WORKERS", "5"))
SUMMARIZE_WORKER_COUNT = int(os.getenv("SUMMARIZE_WORKERS", str(os.cpu_count() or 4)))

BATCH_SIZE = int(os.getenv("WORKFLOW_BATCH_SIZE", "50"))
MAX_CONCURRENT_URLS = int(os.getenv("MAX_CONCURRENT_URLS", "10"))

# ---------------------------------------------------------------------------
# Timeout derivation
# ---------------------------------------------------------------------------
# Each UrlWorkflow child runs this pipeline sequentially:
#   fetch_url -> parse_records -> EnrichmentWorkflow(child) -> summarize_records
#
# Time budgets:
#   fetch_url:     ACTIVITY_TIMEOUT (60s)
#   parse_records: ACTIVITY_TIMEOUT (60s)
#   enrichment:    up to ENRICHMENT_TIMEOUT_BUDGET (300s for 20 articles)
#   summarize:     ACTIVITY_TIMEOUT (60s)
#
# CHILD_WORKFLOW_TIMEOUT must be >= sum of all stages so the parent doesn't
# get killed before children finish. We add a safety margin.
# ---------------------------------------------------------------------------
ACTIVITY_TIMEOUT = timedelta(seconds=float(os.getenv("ACTIVITY_TIMEOUT_SECONDS", "60")))
MAX_ACTIVITY_RETRIES = int(os.getenv("ACTIVITY_MAX_RETRIES", "1"))
NUM_ACTIVITIES = 3
ENRICHMENT_TIMEOUT_BUDGET = 300  # budget for CHILD_WORKFLOW_TIMEOUT derivation (actual enrichment uses 30s/article, capped at 300s)
_activity_budget = ACTIVITY_TIMEOUT.total_seconds() * MAX_ACTIVITY_RETRIES * NUM_ACTIVITIES
_child_timeout_seconds = _activity_budget + ENRICHMENT_TIMEOUT_BUDGET + 60  # 60s safety margin
CHILD_WORKFLOW_TIMEOUT = timedelta(seconds=float(os.getenv("CHILD_WORKFLOW_TIMEOUT_SECONDS", str(_child_timeout_seconds))))
BATCH_GATHER_TIMEOUT = timedelta(seconds=float(os.getenv("BATCH_GATHER_TIMEOUT_SECONDS", "15")))
RETRY_POLICY = RetryPolicy(
    maximum_attempts=MAX_ACTIVITY_RETRIES,
    initial_interval=timedelta(seconds=1),
    maximum_interval=timedelta(seconds=30),
    backoff_coefficient=2.0,
)
