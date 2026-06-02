import asyncio
import logging

import aiohttp

from src.application.interfaces.fetcher import Fetcher

logger = logging.getLogger(__name__)


class AioHttpFetcher(Fetcher):
    def __init__(
        self,
        timeout_seconds: int = 30,
        max_retries: int = 3,
        backoff_base: float = 1.0,
        connector_limit: int = 100,
    ) -> None:
        self._timeout_seconds = timeout_seconds
        self._max_retries = max_retries
        self._backoff_base = backoff_base
        self._timeout = aiohttp.ClientTimeout(total=timeout_seconds)
        self._session = aiohttp.ClientSession(
            connector=aiohttp.TCPConnector(limit=connector_limit),
            timeout=self._timeout,
        )

    async def fetch(self, url: str) -> str:
        last_exception: Exception | None = None
        for attempt in range(self._max_retries):
            try:
                async with self._session.get(url) as response:
                    response.raise_for_status()
                    text = await response.text()
                    logger.info("Fetched URL", extra={"url": url, "status": response.status})
                    return text
            except Exception as e:
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
