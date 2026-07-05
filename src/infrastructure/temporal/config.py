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
SUMMARIZE_WORKER_COUNT = int(os.getenv("SUMMARIZE_WORKERS", str(os.cpu_count() or 4)))

BATCH_SIZE = int(os.getenv("WORKFLOW_BATCH_SIZE", "50"))
MAX_CONCURRENT_URLS = int(os.getenv("MAX_CONCURRENT_URLS", "10"))

# ---------------------------------------------------------------------------
# Timeout derivation
# ---------------------------------------------------------------------------
# Each UrlWorkflow child runs 3 Temporal activities sequentially:
#   fetch_url -> parse_records -> summarize_records
#
# Each activity has:
#   start_to_close_timeout = ACTIVITY_TIMEOUT  (per-attempt wall clock limit)
#   retry_policy.maximum_attempts = MAX_ACTIVITY_RETRIES
#
# Worst-case time for ONE activity to exhaust all retries:
#   ACTIVITY_TIMEOUT * MAX_ACTIVITY_RETRIES
#
# Worst-case time for the entire UrlWorkflow (all 3 activities, each retrying
# up to MAX_ACTIVITY_RETRIES times):
#   ACTIVITY_TIMEOUT * MAX_ACTIVITY_RETRIES * NUM_ACTIVITIES
#
# WHY THIS MATTERS (the original hang bug):
#   Previously CHILD_WORKFLOW_TIMEOUT was set independently (e.g. to the same
#   value as ACTIVITY_TIMEOUT). When a URL hung (unresponsive server), the
#   activity would hit its per-attempt timeout and Temporal would retry it.
#   With 3 retries at 15s each, the activity needed 45s to fully fail. But if
#   the child workflow timeout was also 15s, Temporal killed the child workflow
#   BEFORE all retries finished. The activity's exception handler -- which
#   marks the task as "failed" in the DB -- never ran. The task stayed
#   "pending" in the DB forever, even though the workflow considered it done.
#   The job could never reach "completed + failed == total_tasks" because of
#   this phantom pending task, so the status stayed stuck at "running".
#
# By deriving CHILD_WORKFLOW_TIMEOUT from the activity budget, we guarantee
# every activity can exhaust all retries before the child is killed, so the
# DB is always updated correctly.
# ---------------------------------------------------------------------------
ACTIVITY_TIMEOUT = timedelta(minutes=float(os.getenv("ACTIVITY_TIMEOUT_MINUTES", "0.25")))
MAX_ACTIVITY_RETRIES = int(os.getenv("ACTIVITY_MAX_RETRIES", "3"))
NUM_ACTIVITIES = 3
_child_timeout_seconds = ACTIVITY_TIMEOUT.total_seconds() * MAX_ACTIVITY_RETRIES * NUM_ACTIVITIES
CHILD_WORKFLOW_TIMEOUT = timedelta(seconds=float(os.getenv("CHILD_WORKFLOW_TIMEOUT_SECONDS", str(_child_timeout_seconds))))
BATCH_GATHER_TIMEOUT = timedelta(minutes=float(os.getenv("BATCH_GATHER_TIMEOUT_MINUTES", "0.25")))
RETRY_POLICY = RetryPolicy(
    maximum_attempts=MAX_ACTIVITY_RETRIES,
    initial_interval=timedelta(seconds=1),
    maximum_interval=timedelta(seconds=30),
    backoff_coefficient=2.0,
)
