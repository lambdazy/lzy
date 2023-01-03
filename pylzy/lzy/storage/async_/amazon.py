from typing import BinaryIO, cast, Callable, Optional, Any

from aioboto3 import Session
from botocore.exceptions import ClientError

from lzy.storage.api import S3Credentials, AsyncStorageClient
from lzy.storage.url import Scheme, bucket_from_uri, uri_from_bucket


class AmazonClient(AsyncStorageClient):
    scheme = Scheme.s3

    def __init__(self, credentials: S3Credentials):
        self.__credentials = credentials

    def _get_client_context(self):
        return Session().client(
            "s3",
            aws_access_key_id=self.__credentials.access_token,
            aws_secret_access_key=self.__credentials.secret_token,
            endpoint_url=self.__credentials.endpoint,
        )

    async def size_in_bytes(self, uri: str) -> int:
        async with self._get_client_context() as client:
            bucket, key = bucket_from_uri(self.scheme, uri)
            head = await client.head_object(Bucket=bucket, Key=key)
            return cast(int, head['ContentLength'])

    async def read(self, url: str, data: BinaryIO, progress: Optional[Callable[[int], Any]] = None):
        async with self._get_client_context() as client:
            bucket, key = bucket_from_uri(self.scheme, url)
            await client.download_fileobj(bucket, key, data, Callback=progress)

    async def write(self, url: str, data: BinaryIO, progress: Optional[Callable[[int], Any]] = None) -> str:
        async with self._get_client_context() as client:
            bucket, key = bucket_from_uri(self.scheme, url)
            await client.upload_fileobj(data, bucket, key, Callback=progress)
            return uri_from_bucket(self.scheme, bucket, key)

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

    async def sign_storage_uri(self, url: str) -> str:
        container, blob = bucket_from_uri(self.scheme, url)
        async with self._get_client_context() as client:
            url = await client.generate_presigned_url(
                "get_object",
                Params={"Bucket": container, "Key": blob},
                ExpiresIn=3600,  # 1 h
            )
            return cast(str, url)
