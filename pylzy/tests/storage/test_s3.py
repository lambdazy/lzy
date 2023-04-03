import asyncio
from io import BytesIO
from unittest import TestCase

import requests
# noinspection PyPackageRequirements
from moto.moto_server.threaded_moto_server import ThreadedMotoServer

from tests.api.v1.utils import create_bucket
from lzy.storage.api import S3Credentials
from lzy.storage.async_.s3 import S3Client


class S3Tests(TestCase):
    def setUp(self) -> None:
        self.service = ThreadedMotoServer(port=12345)
        self.service.start()
        self.endpoint_url = "http://localhost:12345"
        asyncio.run(create_bucket(self.endpoint_url))
        self.client = S3Client(credentials=S3Credentials(self.endpoint_url, access_key_id="", secret_access_key=""))

    def tearDown(self) -> None:
        self.service.stop()

    def test_presigned_url(self):
        uri = "s3://bucket/key"
        with BytesIO(b"42") as f:
            asyncio.run(self.client.write(uri, f))

        presigned_url = asyncio.run(self.client.sign_storage_uri(uri))

        response = requests.get(presigned_url, stream=True)
        data = next(iter(response.iter_content(16)))
        self.assertEqual(data, b"42")
