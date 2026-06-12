from datetime import timedelta

from temporalio.common import RetryPolicy

WORKFLOW_NAME = "job-workflow"

WORKFLOW_QUEUE = "xml-feed-workflow-queue"
FETCH_QUEUE = "xml-feed-fetch-queue"
PARSE_QUEUE = "xml-feed-parse-queue"
SUMMARIZE_QUEUE = "xml-feed-summarize-queue"

FETCH_WORKER_COUNT = 5
PARSE_WORKER_COUNT = 5
SUMMARIZE_WORKER_COUNT = 5

ACTIVITY_TIMEOUT = timedelta(minutes=5)
RETRY_POLICY = RetryPolicy(
    maximum_attempts=3,
    initial_interval=timedelta(seconds=1),
    maximum_interval=timedelta(seconds=30),
    backoff_coefficient=2.0,
)
