import logging
import os
from typing import BinaryIO
from urllib import parse

import boto3
from botocore.exceptions import ClientError

from lzy.storage.api import AmazonCredentials
from lzy.storage.deprecated.storage_client import StorageClient
from lzy.storage.url import Scheme, bucket_from_uri


class AmazonClient(StorageClient):
    scheme = Scheme.s3

    def __init__(self, credentials: AmazonCredentials):
        super().__init__()
        self._client = boto3.client(
            "s3",
            aws_access_key_id=credentials.access_token,
            aws_secret_access_key=credentials.secret_token,
            endpoint_url=credentials.endpoint,
        )
        self.__logger = logging.getLogger(self.__class__.__name__)

    def read(self, url: str, dest: BinaryIO) -> None:
        uri = parse.urlparse(url)
        assert uri.scheme == "s3"
        bucket, key = bucket_from_uri(self.scheme, url)
        self._client.download_fileobj(bucket, key, dest)

    def write(self, bucket: str, key: str, data: BinaryIO) -> str:
        self._client.upload_fileobj(data, bucket, key)
        return self.generate_uri(bucket, key)

    def blob_exists(self, container: str, blob: str) -> bool:
        # ridiculous, but botocore does not have a way to check for resource existence.
        # Try-except seems to be the best solution for now.
        try:
            self._client.head_object(Bucket=container, Key=blob)
            return True
        except ClientError:
            return False

    def generate_uri(self, bucket: str, key: str) -> str:
        path = os.path.join(bucket, key)
        return f"s3:/{path}"
