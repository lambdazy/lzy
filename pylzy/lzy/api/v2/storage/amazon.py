import io
from contextlib import AsyncExitStack
from pathlib import Path
from typing import AsyncIterator, BinaryIO

from aioboto3 import Session
from botocore.exceptions import ClientError

from lzy.api.v2.storage.url import Scheme, bucket_from_url, url_from_bucket
from lzy.storage.credentials import AmazonCredentials


class AmazonClient:
    scheme = Scheme.s3

    def __init__(self, credentials: AmazonCredentials):
        self.session = Session()
        self.resources = AsyncExitStack()

        # TODO[ottergottaott]: close this client
        self._client = self.session.client(
            "s3",
            aws_access_key_id=credentials.access_token,
            aws_secret_access_key=credentials.secret_token,
            endpoint_url=credentials.endpoint,
        )

    async def read(self, url: str, data: BinaryIO):
        async for chunk in self.blob_iter(url):
            data.write(chunk)

    async def write(self, bucket: str, key: str, data: BinaryIO) -> str:
        await self._client.upload_fileobj(data, bucket, key)
        return url_from_bucket(self.scheme, bucket, key)

    async def blob_exists(self, container: str, blob: str) -> bool:
        # ridiculous, but botocore does not have a way to check for resource existence.
        # Try-except seems to be the best solution for now.
        try:
            await self._client.head_object(
                Bucket=container,
                Key=blob,
            )
            return True
        except ClientError:
            return False

    async def blob_iter(
        self,
        url: str,
        chunk_size: int = 69 * 1024,
    ) -> AsyncIterator[bytes]:
        bucket, key = bucket_from_url(self.scheme, url)

        s3_obj = await self._client.get_object(
            Bucket=bucket,
            Key=key,
        )

        async with s3_obj["Body"] as stream:
            while True:
                data = await stream.read(chunk_size)
                if not data:
                    return

                yield data
