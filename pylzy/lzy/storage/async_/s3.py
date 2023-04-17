import asyncio
import warnings
from typing import BinaryIO, cast, Callable, Optional, Any

from aioboto3 import Session
from aiobotocore.response import AioReadTimeoutError  # type: ignore
from aiohttp import ServerTimeoutError
from botocore.config import Config
from botocore.exceptions import ClientError, EndpointConnectionError

from lzy.storage.api import S3Credentials, AsyncStorageClient
from lzy.storage.url import Scheme, bucket_from_uri, uri_from_bucket


def _empty_progress(i: int, b: bool):
    pass


class S3Client(AsyncStorageClient):
    scheme = Scheme.s3
    __retry_count = 12000
    __retry_period_s = 1

    def __init__(self, credentials: S3Credentials):
        self.__credentials = credentials

    def _get_client_context(self):
        return Session().client(
            "s3",
            aws_access_key_id=self.__credentials.access_key_id,
            aws_secret_access_key=self.__credentials.secret_access_key,
            endpoint_url=self.__credentials.endpoint,
            config=Config(
                retries={
                    "max_attempts": self.__retry_count,
                    "mode": "adaptive"
                }
            )
        )

    async def size_in_bytes(self, uri: str) -> int:
        async with self._get_client_context() as client:
            bucket, key = bucket_from_uri(self.scheme, uri)
            head = await client.head_object(Bucket=bucket, Key=key)
            return cast(int, head['ContentLength'])

    async def read(self, uri: str, data: BinaryIO, progress: Optional[Callable[[int, bool], Any]] = None):
        real_progress = progress if progress is not None else _empty_progress

        current_count = 0
        exc: Optional[Exception] = None
        while current_count < self.__retry_count:
            try:
                async with self._get_client_context() as client:
                    bucket, key = bucket_from_uri(self.scheme, uri)
                    data.seek(0)
                    real_progress(0, True)
                    await client.download_fileobj(bucket, key, data, Callback=lambda x: real_progress(x, False))
                    return
            except (ServerTimeoutError, AioReadTimeoutError, EndpointConnectionError) as e:
                warnings.warn(
                    f"Lost connection while reading data from {uri}."
                    f" Retrying, attempt {current_count}/{self.__retry_count}")
                await asyncio.sleep(self.__retry_period_s)
                current_count += 1
                exc = e

        if exc is not None:
            raise RuntimeError("Cannot read data from s3") from exc
        else:
            raise RuntimeError("Cannot read data from s3")

    async def write(self, uri: str, data: BinaryIO, progress: Optional[Callable[[int, bool], Any]] = None) -> str:
        real_progress = progress if progress is not None else _empty_progress

        current_count = 0
        exc: Optional[Exception] = None
        while current_count < self.__retry_count:
            try:
                async with self._get_client_context() as client:
                    bucket, key = bucket_from_uri(self.scheme, uri)
                    data.seek(0)
                    real_progress(0, True)
                    await client.upload_fileobj(data, bucket, key, Callback=lambda x: real_progress(x, False))
                    return uri_from_bucket(self.scheme, bucket, key)
            except (ServerTimeoutError, EndpointConnectionError) as e:
                warnings.warn(
                    f"Lost connection while reading data from {uri}."
                    f" Retrying, attempt {current_count}/{self.__retry_count}")
                await asyncio.sleep(self.__retry_period_s)
                current_count += 1
                exc = e

        if exc is not None:
            raise RuntimeError("Cannot write data into s3") from exc
        else:
            raise RuntimeError("Cannot write data into s3")

    async def copy(self, from_uri: str, to_uri: str) -> None:
        bucket_from, key_from = bucket_from_uri(self.scheme, from_uri)
        bucket_to, key_to = bucket_from_uri(self.scheme, to_uri)
        async with self._get_client_context() as client:
            await client.copy_object(Bucket=bucket_to, CopySource=f"{bucket_from}/{key_from}", Key=key_to)

    async def blob_exists(self, uri: str) -> bool:
        container, blob = bucket_from_uri(self.scheme, uri)

        # ridiculous, but botocore does not have a way to check for resource existence.
        # Try-except seems to be the best solution for now.
        try:
            async with self._get_client_context() as client:
                await client.head_object(
                    Bucket=container,
                    Key=blob,
                )
                return True
        except ClientError:
            return False

    async def sign_storage_uri(self, uri: str) -> str:
        container, blob = bucket_from_uri(self.scheme, uri)
        async with self._get_client_context() as client:
            url = await client.generate_presigned_url(
                "get_object",
                Params={"Bucket": container, "Key": blob},
                ExpiresIn=259200,  # 3 days
            )
            return cast(str, url)
