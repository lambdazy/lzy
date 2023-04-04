import asyncio
import uuid
from unittest import TestCase

# noinspection PyPackageRequirements
from moto.moto_server.threaded_moto_server import ThreadedMotoServer

from lzy.proxy.either import Left, Right
from tests.api.v1.mocks import SerializerRegistryMock, NotAvailablePrimitiveSerializer
from tests.api.v1.utils import create_bucket
from lzy.api.v1.snapshot import DefaultSnapshot
from lzy.serialization.registry import LzySerializerRegistry
from lzy.storage import api as storage
from lzy.storage.registry import DefaultStorageRegistry


class SnapshotTests(TestCase):
    def setUp(self) -> None:
        self.service = ThreadedMotoServer(port=12345)
        self.service.start()
        self.endpoint_url = "http://localhost:12345"
        asyncio.run(create_bucket(self.endpoint_url))

        storage_config = storage.Storage(
            uri="s3://bucket/prefix",
            credentials=storage.S3Credentials(self.endpoint_url, access_key_id="", secret_access_key="")
        )
        self.storages = DefaultStorageRegistry()
        self.storages.register_storage("storage", storage_config, default=True)

        serializers = LzySerializerRegistry()
        storage_uri = f"{storage_config.uri}/lzy_runs/some_wf/inputs"
        self.snapshot = DefaultSnapshot(serializers, storage_uri, self.storages.client("storage"), "storage")

    def tearDown(self) -> None:
        self.service.stop()

    def test_put_get(self):
        entry = self.snapshot.create_entry("name", str)

        asyncio.run(self.snapshot.put_data(entry.id, "some_str"))
        ret = asyncio.run(self.snapshot.get_data(entry.id))

        self.assertEqual(Left("some_str"), ret)

    def test_get_nonexistent_entry(self):
        with self.assertRaisesRegex(ValueError, "does not exist"):
            asyncio.run(self.snapshot.get_data(str(uuid.uuid4())))

    def test_put_nonexistent_entry(self):
        with self.assertRaisesRegex(ValueError, "does not exist"):
            asyncio.run(self.snapshot.put_data(str(uuid.uuid4()), "some_str"))

    def test_get_not_uploaded_entry(self):
        entry = self.snapshot.create_entry("name", str)
        entry.storage_uri = f"{self.storages.config('storage').uri}/name"
        ret = asyncio.run(self.snapshot.get_data(entry.id))
        self.assertIsInstance(ret, Right)

    def test_update(self):
        entry = self.snapshot.create_entry("name", str)
        entry.storage_uri = f"{self.storages.config('storage').uri}/name"
        entry.storage_uri = f"{self.storages.config('storage').uri}/name2"

        asyncio.run(self.snapshot.put_data(entry.id, "some_str"))
        ret = asyncio.run(self.snapshot.get_data(entry.id))

        # self.assertEqual(f"{self.storages.config('storage').uri}/name2", self.snapshot.get(entry.id).storage_uri)
        self.assertEqual(Left("some_str"), ret)

    def test_serializer_not_found(self):
        serializers = SerializerRegistryMock()
        storage_uri = f"{self.storages.config('storage').uri}/lzy_runs/some_wf/inputs"
        snapshot = DefaultSnapshot(serializers, storage_uri, self.storages.client("storage"), "storage")

        with self.assertRaisesRegex(TypeError, "Cannot find serializer for type"):
            snapshot.create_entry("name", str)

    def test_serializer_not_available(self):
        serializers = SerializerRegistryMock()
        serializers.register_serializer(NotAvailablePrimitiveSerializer())
        storage_uri = f"{self.storages.config('storage').uri}/lzy_runs/some_wf/inputs"
        snapshot = DefaultSnapshot(serializers, storage_uri, self.storages.client("storage"), "storage")

        with self.assertRaisesRegex(TypeError, "is not available, please install"):
            snapshot.create_entry("name", str)
