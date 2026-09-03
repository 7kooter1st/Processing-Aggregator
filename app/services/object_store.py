from __future__ import annotations

import asyncio
import hashlib
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import BinaryIO, Protocol

from app.config import settings

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class StoredObject:
    key: str
    sha256: str
    size_bytes: int
    content_type: str


class ObjectStore(Protocol):
    async def put_bytes(
        self,
        key: str,
        data: bytes,
        content_type: str = "application/octet-stream",
    ) -> StoredObject: ...

    async def put_fileobj(
        self,
        key: str,
        stream: BinaryIO,
        content_type: str = "application/octet-stream",
        *,
        max_bytes: int | None = None,
    ) -> StoredObject: ...

    async def get_bytes(self, key: str) -> bytes: ...

    async def delete(self, key: str) -> None: ...

    async def exists(self, key: str) -> bool: ...

    async def ping(self) -> bool: ...


class LocalObjectStore:
    """Filesystem object store with S3-like keys."""

    def __init__(self, root: Path | None = None) -> None:
        self._root = Path(root or settings.object_store_root)

    def _path_for(self, key: str) -> Path:
        relative = Path(key)
        if relative.is_absolute() or ".." in relative.parts:
            raise ValueError(f"Invalid object key: {key}")
        return (self._root / relative).resolve()

    async def put_bytes(
        self,
        key: str,
        data: bytes,
        content_type: str = "application/octet-stream",
    ) -> StoredObject:
        def _write() -> StoredObject:
            path = self._path_for(key)
            path.parent.mkdir(parents=True, exist_ok=True)
            tmp = path.with_suffix(path.suffix + ".tmp")
            tmp.write_bytes(data)
            tmp.replace(path)
            digest = hashlib.sha256(data).hexdigest()
            meta = path.with_suffix(path.suffix + ".meta")
            meta.write_text(f"{content_type}\n{digest}\n{len(data)}", encoding="utf-8")
            return StoredObject(
                key=key,
                sha256=digest,
                size_bytes=len(data),
                content_type=content_type,
            )

        return await asyncio.to_thread(_write)

    async def put_fileobj(
        self,
        key: str,
        stream: BinaryIO,
        content_type: str = "application/octet-stream",
        *,
        max_bytes: int | None = None,
    ) -> StoredObject:
        def _write() -> StoredObject:
            path = self._path_for(key)
            path.parent.mkdir(parents=True, exist_ok=True)
            tmp = path.with_suffix(path.suffix + ".tmp")
            hasher = hashlib.sha256()
            size = 0
            with tmp.open("wb") as handle:
                while True:
                    chunk = stream.read(64 * 1024)
                    if not chunk:
                        break
                    size += len(chunk)
                    if max_bytes is not None and size > max_bytes:
                        handle.close()
                        tmp.unlink(missing_ok=True)
                        raise ValueError("Object exceeds max_bytes")
                    hasher.update(chunk)
                    handle.write(chunk)
            tmp.replace(path)
            digest = hasher.hexdigest()
            meta = path.with_suffix(path.suffix + ".meta")
            meta.write_text(f"{content_type}\n{digest}\n{size}", encoding="utf-8")
            return StoredObject(
                key=key,
                sha256=digest,
                size_bytes=size,
                content_type=content_type,
            )

        return await asyncio.to_thread(_write)

    async def get_bytes(self, key: str) -> bytes:
        path = self._path_for(key)
        return await asyncio.to_thread(path.read_bytes)

    async def delete(self, key: str) -> None:
        path = self._path_for(key)
        meta = path.with_suffix(path.suffix + ".meta")

        def _delete() -> None:
            path.unlink(missing_ok=True)
            meta.unlink(missing_ok=True)

        await asyncio.to_thread(_delete)

    async def exists(self, key: str) -> bool:
        path = self._path_for(key)
        return await asyncio.to_thread(path.is_file)

    async def ping(self) -> bool:
        try:
            await asyncio.to_thread(self._root.mkdir, parents=True, exist_ok=True)
            probe = self._root / ".ready"
            await asyncio.to_thread(probe.write_text, "ok", "utf-8")
            return True
        except Exception:
            logger.exception("[OBJECT STORE] local ping failed")
            return False

    async def delete_prefix(self, prefix: str) -> None:
        root = self._path_for(prefix)

        def _rm() -> None:
            if root.is_dir():
                import shutil

                shutil.rmtree(root, ignore_errors=True)

        await asyncio.to_thread(_rm)


class S3ObjectStore:
    def __init__(self) -> None:
        self._bucket = settings.s3_bucket
        self._endpoint = settings.s3_endpoint_url
        self._access_key = settings.s3_access_key
        self._secret_key = settings.s3_secret_key
        self._region = settings.s3_region

    def _client(self):
        import boto3
        from botocore.config import Config as BotoConfig

        return boto3.client(
            "s3",
            endpoint_url=self._endpoint or None,
            aws_access_key_id=self._access_key or None,
            aws_secret_access_key=self._secret_key or None,
            region_name=self._region,
            config=BotoConfig(s3={"addressing_style": "path"}),
        )

    async def put_bytes(
        self,
        key: str,
        data: bytes,
        content_type: str = "application/octet-stream",
    ) -> StoredObject:
        digest = hashlib.sha256(data).hexdigest()

        def _put() -> None:
            self._client().put_object(
                Bucket=self._bucket,
                Key=key,
                Body=data,
                ContentType=content_type,
            )

        await asyncio.to_thread(_put)
        return StoredObject(
            key=key,
            sha256=digest,
            size_bytes=len(data),
            content_type=content_type,
        )

    async def put_fileobj(
        self,
        key: str,
        stream: BinaryIO,
        content_type: str = "application/octet-stream",
        *,
        max_bytes: int | None = None,
    ) -> StoredObject:
        data = await asyncio.to_thread(stream.read)
        if max_bytes is not None and len(data) > max_bytes:
            raise ValueError("Object exceeds max_bytes")
        return await self.put_bytes(key, data, content_type)

    async def get_bytes(self, key: str) -> bytes:
        def _get() -> bytes:
            response = self._client().get_object(Bucket=self._bucket, Key=key)
            return response["Body"].read()

        return await asyncio.to_thread(_get)

    async def delete(self, key: str) -> None:
        def _delete() -> None:
            self._client().delete_object(Bucket=self._bucket, Key=key)

        await asyncio.to_thread(_delete)

    async def exists(self, key: str) -> bool:
        def _head() -> bool:
            try:
                self._client().head_object(Bucket=self._bucket, Key=key)
                return True
            except Exception:
                return False

        return await asyncio.to_thread(_head)

    async def ping(self) -> bool:
        def _head() -> bool:
            self._client().head_bucket(Bucket=self._bucket)
            return True

        try:
            return await asyncio.to_thread(_head)
        except Exception:
            logger.exception("[OBJECT STORE] s3 ping failed")
            return False

    async def delete_prefix(self, prefix: str) -> None:
        def _delete() -> None:
            client = self._client()
            token = None
            while True:
                kwargs = {"Bucket": self._bucket, "Prefix": prefix}
                if token:
                    kwargs["ContinuationToken"] = token
                page = client.list_objects_v2(**kwargs)
                objects = [
                    {"Key": item["Key"]} for item in page.get("Contents") or []
                ]
                if objects:
                    client.delete_objects(
                        Bucket=self._bucket,
                        Delete={"Objects": objects},
                    )
                if not page.get("IsTruncated"):
                    break
                token = page.get("NextContinuationToken")

        await asyncio.to_thread(_delete)


_store: LocalObjectStore | S3ObjectStore | None = None


def get_object_store() -> LocalObjectStore | S3ObjectStore:
    global _store
    if _store is None:
        if settings.object_store_backend == "s3":
            _store = S3ObjectStore()
        else:
            _store = LocalObjectStore()
    return _store
