import logging
from typing import AsyncIterator, BinaryIO

from aioboto3 import Session
from botocore.exceptions import ClientError

from lzy.storage.api import AmazonCredentials, AsyncStorageClient
from lzy.storage.url import Scheme, bucket_from_url, url_from_bucket

_LOG = logging.getLogger(__name__)


class AmazonClient(AsyncStorageClient):
    scheme = Scheme.s3

    def __init__(self, credentials: AmazonCredentials):
        self.__credentials = credentials

    def _get_client_context(self):
        return Session().client(
            "s3",
            aws_access_key_id=self.__credentials.access_token,
            aws_secret_access_key=self.__credentials.secret_token,
            endpoint_url=self.__credentials.endpoint,
        )

    async def read(self, url: str, data: BinaryIO):
        async with self._get_client_context() as client:
            bucket, key = bucket_from_url(self.scheme, url)
            await client.download_fileobj(bucket, key, data)

    async def write(self, url: str, data: BinaryIO) -> str:
        async with self._get_client_context() as client:
            bucket, key = bucket_from_url(self.scheme, url)
            await client.upload_fileobj(data, bucket, key)
            return url_from_bucket(self.scheme, bucket, key)

    async def blob_exists(self, container: str, blob: str) -> bool:
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

    async def bucket_exists(self, bucket: str) -> bool:
        try:
            async with self._get_client_context() as client:
                await client.get_bucket_acl(Bucket=bucket)
                return True
        except ClientError:
            return False

    def generate_uri(self, container: str, blob: str) -> str:
        return url_from_bucket(self.scheme, container, blob)
