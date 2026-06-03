import asyncio
import logging

import aiohttp

from src.application.interfaces.fetcher import Fetcher

logger = logging.getLogger(__name__)

_HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-User": "?1",
    "Upgrade-Insecure-Requests": "1",
}

_PERMANENT_STATUSES = {403, 404, 410}


class AioHttpFetcher(Fetcher):
    def __init__(
        self,
        timeout_seconds: int = 30,
        max_retries: int = 3,
        backoff_base: float = 1.0,
        connector_limit: int = 100,
        connector_limit_per_host: int = 2,
    ) -> None:
        self._timeout_seconds = timeout_seconds
        self._max_retries = max_retries
        self._backoff_base = backoff_base
        self._timeout = aiohttp.ClientTimeout(total=timeout_seconds)
        self._session = aiohttp.ClientSession(
            connector=aiohttp.TCPConnector(
                limit=connector_limit, limit_per_host=connector_limit_per_host
            ),
            timeout=self._timeout,
            headers=dict(_HEADERS),
        )

    async def fetch(self, url: str) -> str:
        last_exception: Exception | None = None
        for attempt in range(self._max_retries):
            try:
                async with self._session.get(url) as response:
                    text = await response.text()
                    if response.status in _PERMANENT_STATUSES:
                        logger.warning(
                            "Permanent failure %s, not retrying: %s",
                            response.status,
                            url,
                        )
                        raise RuntimeError(
                            f"Permanent failure HTTP {response.status} for {url}"
                        ) from aiohttp.ClientResponseError(
                            response.request_info, response.history,
                            status=response.status,
                        )
                    if response.status == 429:
                        retry_after = response.headers.get("Retry-After")
                        delay = int(retry_after) if retry_after and retry_after.isdigit() else 5 * (2**attempt)
                        logger.warning(
                            "Rate limited (429), retrying after %ss: %s",
                            delay,
                            url,
                            extra={"url": url, "attempt": attempt + 1, "delay": delay},
                        )
                        if attempt < self._max_retries - 1:
                            await asyncio.sleep(delay)
                            continue
                        raise aiohttp.ClientResponseError(
                            response.request_info,
                            response.history,
                            status=429,
                            message=f"Rate limited after {self._max_retries} retries",
                        )
                    if response.status >= 500:
                        logger.warning(
                            "Server error %s, returning body anyway: %s",
                            response.status,
                            url,
                        )
                    else:
                        logger.info("Fetched URL", extra={"url": url, "status": response.status})
                    return text
            except asyncio.CancelledError:
                raise
            except Exception as e:
                if isinstance(e, RuntimeError) and "Permanent failure" in str(e):
                    raise
                last_exception = e
                logger.warning(
                    "Fetch attempt failed: %s",
                    str(e),
                    extra={"url": url, "attempt": attempt + 1, "error": str(e)},
                )
                if attempt < self._max_retries - 1:
                    delay = self._backoff_base * (2**attempt)
                    await asyncio.sleep(delay)
        raise RuntimeError(f"Failed to fetch {url} after {self._max_retries} retries") from last_exception

    async def close(self) -> None:
        await self._session.close()
