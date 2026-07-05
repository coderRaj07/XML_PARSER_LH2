import asyncio
import io
import logging
import os
from typing import Optional

logger = logging.getLogger(__name__)


class S3Storage:
    def __init__(
        self,
        endpoint_url: Optional[str] = None,
        bucket: str = "xml-feeds",
        access_key_id: Optional[str] = None,
        secret_access_key: Optional[str] = None,
        region: str = "us-east-1",
    ) -> None:
        self._endpoint_url = endpoint_url or os.getenv("S3_ENDPOINT_URL", "http://minio:9000")
        self._bucket = bucket or os.getenv("S3_BUCKET", "xml-feeds")
        self._access_key_id = access_key_id or os.getenv("S3_ACCESS_KEY_ID", "minioadmin")
        self._secret_access_key = secret_access_key or os.getenv("S3_SECRET_ACCESS_KEY", "minioadmin")
        self._region = region
        self._client = None

    async def _get_client(self):
        if self._client is None:
            import boto3

            self._client = await asyncio.to_thread(
                boto3.client,
                "s3",
                endpoint_url=self._endpoint_url,
                aws_access_key_id=self._access_key_id,
                aws_secret_access_key=self._secret_access_key,
                region_name=self._region,
            )
            await self._ensure_bucket()
        return self._client

    async def _ensure_bucket(self) -> None:
        client = self._client
        try:
            existing = await asyncio.to_thread(client.list_buckets)
            buckets = [b["Name"] for b in existing.get("Buckets", [])]
            if self._bucket not in buckets:
                await asyncio.to_thread(client.create_bucket, Bucket=self._bucket)
                logger.info("s3_bucket_created", extra={"bucket": self._bucket})
        except Exception as e:
            logger.warning("s3_bucket_check_failed", extra={"error": str(e)})

    async def store(self, key: str, data: str) -> None:
        client = await self._get_client()
        body = data.encode("utf-8")
        await asyncio.to_thread(
            client.put_object,
            Bucket=self._bucket,
            Key=key,
            Body=body,
            ContentType="application/xml",
        )
        logger.debug("s3_store_completed", extra={"key": key, "size": len(body)})

    async def store_stream(self, key: str, data: io.IOBase, content_type: str = "application/octet-stream") -> None:
        client = await self._get_client()
        await asyncio.to_thread(
            client.put_object,
            Bucket=self._bucket,
            Key=key,
            Body=data,
            ContentType=content_type,
        )
        logger.debug("s3_store_stream_completed", extra={"key": key})

    async def retrieve(self, key: str) -> str:
        client = await self._get_client()
        response = await asyncio.to_thread(
            client.get_object,
            Bucket=self._bucket,
            Key=key,
        )
        body = response["Body"].read().decode("utf-8")
        logger.debug("s3_retrieve_completed", extra={"key": key, "size": len(body)})
        return body

    async def get_bytes(self, key: str) -> bytes:
        """Read entire S3 object into bytes (safe cross-thread)."""
        client = await self._get_client()
        response = await asyncio.to_thread(
            client.get_object,
            Bucket=self._bucket,
            Key=key,
        )
        data = response["Body"].read()
        logger.debug("s3_get_bytes", extra={"key": key, "size": len(data)})
        return data

    def _make_content_key(self, job_id: str, task_id: str, record_id: str) -> str:
        return f"content/{job_id}/{task_id}/{record_id}.gz"

    def build_key(self, job_id: str, task_id: str) -> str:
        return f"feeds/{job_id}/{task_id}.xml"
