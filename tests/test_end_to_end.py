import asyncio
import sys
import time
from pathlib import Path

import httpx

API_BASE = "http://localhost:8000"
URLS_FILE = Path(__file__).parent / "test_urls.txt"
POLL_INTERVAL = 5
MAX_WAIT = 600


def load_urls() -> list[str]:
    lines = URLS_FILE.read_text().strip().splitlines()
    return [line.strip() for line in lines if line.strip()]


async def create_job(client: httpx.AsyncClient, urls: list[str]) -> str:
    resp = await client.post("/jobs", json={"urls": urls})
    resp.raise_for_status()
    job_id = resp.json()["job_id"]
    print(f"Created job {job_id} with {len(urls)} URLs")
    return job_id


async def wait_for_completion(client: httpx.AsyncClient, job_id: str) -> dict:
    start = time.time()
    while True:
        resp = await client.get(f"/jobs/{job_id}")
        resp.raise_for_status()
        data = resp.json()
        elapsed = time.time() - start
        status, total, completed, failed = (
            data["status"],
            data["total"],
            data["completed"],
            data["failed"],
        )
        in_progress = total - completed - failed
        sys.stdout.write(
            f"\r  elapsed={elapsed:.0f}s  "
            f"total={total}  completed={completed}  "
            f"failed={failed}  in_progress={in_progress}  "
            f"status={status}   "
        )
        sys.stdout.flush()

        if status in ("completed", "failed") or elapsed > MAX_WAIT:
            print()
            return data

        await asyncio.sleep(POLL_INTERVAL)


async def fetch_tasks(client: httpx.AsyncClient, job_id: str) -> list[dict]:
    resp = await client.get(f"/jobs/{job_id}/tasks")
    resp.raise_for_status()
    return resp.json()


async def main() -> None:
    urls = load_urls()
    print(f"Loaded {len(urls)} URLs from {URLS_FILE}")

    async with httpx.AsyncClient(base_url=API_BASE, timeout=30) as client:
        job_id = await create_job(client, urls)
        status_data = await wait_for_completion(client, job_id)

        print(f"\nFinal job status: {status_data}")

        tasks = await fetch_tasks(client, job_id)
        failed_tasks = [t for t in tasks if t.get("status") == "FAILED"]
        completed_tasks = [t for t in tasks if t.get("status") == "COMPLETED"]

        print(f"\nResults: {len(completed_tasks)} completed, {len(failed_tasks)} failed")
        if failed_tasks:
            print("\nFailed tasks:")
            for t in failed_tasks[:20]:
                print(f"  {t['url']}: {t.get('error', 'unknown')}")
            if len(failed_tasks) > 20:
                print(f"  ... and {len(failed_tasks) - 20} more")

    total_time = time.time() - globals().get("_start", time.time())
    print(f"\nTotal time: {total_time:.1f}s")


if __name__ == "__main__":
    globals()["_start"] = time.time()
    asyncio.run(main())
