import os
from datetime import timedelta

from temporalio.common import RetryPolicy

WORKFLOW_NAME = "job-workflow"

WORKFLOW_QUEUE = os.getenv("TEMPORAL_WORKFLOW_QUEUE", "xml-feed-workflow-queue")
FETCH_QUEUE = os.getenv("TEMPORAL_FETCH_QUEUE", "xml-feed-fetch-queue")
PARSE_QUEUE = os.getenv("TEMPORAL_PARSE_QUEUE", "xml-feed-parse-queue")
SUMMARIZE_QUEUE = os.getenv("TEMPORAL_SUMMARIZE_QUEUE", "xml-feed-summarize-queue")

FETCH_WORKER_COUNT = int(os.getenv("FETCH_WORKERS", "5"))
PARSE_WORKER_COUNT = int(os.getenv("PARSE_WORKERS", "5"))
SUMMARIZE_WORKER_COUNT = int(os.getenv("SUMMARIZE_WORKERS", "5"))

ACTIVITY_TIMEOUT = timedelta(minutes=int(os.getenv("ACTIVITY_TIMEOUT_MINUTES", "5")))
RETRY_POLICY = RetryPolicy(
    maximum_attempts=int(os.getenv("ACTIVITY_MAX_RETRIES", "3")),
    initial_interval=timedelta(seconds=1),
    maximum_interval=timedelta(seconds=30),
    backoff_coefficient=2.0,
)
