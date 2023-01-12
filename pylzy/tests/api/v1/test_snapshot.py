import asyncio
from io import BytesIO
from unittest import TestCase

import requests

from lzy.api.v1.snapshot import DefaultSnapshot
# noinspection PyPackageRequirements
from moto.moto_server.threaded_moto_server import ThreadedMotoServer

from lzy.proxy.result import Just
from lzy.serialization.registry import LzySerializerRegistry
from lzy.storage import api as storage

from api.v1.utils import create_bucket
from lzy.storage.registry import DefaultStorageRegistry


class SnapshotTests(TestCase):
    def setUp(self) -> None:
        self.service = ThreadedMotoServer(port=12345)
        self.service.start()
        self.endpoint_url = "http://localhost:12345"
        asyncio.run(create_bucket(self.endpoint_url))

        storage_config = storage.Storage(
            uri="s3://bucket/prefix",
            credentials=storage.S3Credentials(
                self.endpoint_url, access_token="", secret_token=""
            ),
        )
        self.storages = DefaultStorageRegistry()
        self.storages.register_storage("storage", storage_config, True)

    def tearDown(self) -> None:
        self.service.stop()

    def test_put_get(self):
        serializers = LzySerializerRegistry()
        snapshot = DefaultSnapshot(serializers, self.storages.client("storage"), "storage")
        entry = snapshot.create_entry("name", str, f"{self.storages.config('storage').uri}/name")

        asyncio.run(snapshot.put_data(entry.id, "some_str"))
        ret = asyncio.run(snapshot.get_data(entry.id))

        self.assertEqual(Just("some_str"), ret)
