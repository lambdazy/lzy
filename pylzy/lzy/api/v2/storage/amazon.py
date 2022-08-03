import io

from contextlib import AsyncExitStack
from pathlib import Path
from typing import BinaryIO

from aioboto3 import Session
from botocore.exceptions import ClientError

from lzy.api.v2.storage.url import bucket_from_url, url_from_bucket, Scheme
from lzy.storage.credentials import AmazonCredentials


class AmazonClient:
    scheme = Scheme.s3

    def __init__(self, credentials: AmazonCredentials):
        super().__init__()
        self.session = Session()
        self.resources = AsyncExitStack()

        self._client = await self.resources.enter_async_context(
            self.session.client(
                "s3",
                aws_access_key_id=credentials.access_token,
                aws_secret_access_key=credentials.secret_token,
                endpoint_url=credentials.endpoint,
            )
        )

    async def read(self, url: str) -> bytes:
        with io.BytesIO() as buf:
            await self._read_into(buf)
            return buf.getvalue()

    async def read_to_file(self, url: str, filepath: Path) -> None:
        with filepath.open("wb") as file:
            await self._read_into(url, file)

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

    async def _read_into(
        self,
        url: str,
        dest: BinaryIO,
        chunk_size: int = 69 * 1024,
    ):
        bucket, key = bucket_from_url(self.scheme, url)

        s3_obj = await self._client.get_object(
            Bucket=bucket,
            Key=key,
        )

        # this will ensure the connection is correctly re-used/closed
        async with s3_obj["Body"] as stream:
            while True:
                data = await stream.read(chunk_size)
                if not data:
                    return

                dest.write(data)
