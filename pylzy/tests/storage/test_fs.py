import asyncio
import tempfile
from io import BytesIO
from unittest import TestCase

from lzy.storage.async_.fs import FsStorageClient


class FsStorageTests(TestCase):
    def setUp(self) -> None:
        self.client = FsStorageClient()

    def test_write_read(self):
        with tempfile.NamedTemporaryFile("w") as file:
            uri = "file://" + file.name
            asyncio.run(self.client.write(uri, BytesIO(b"str")))
            self.assertTrue(asyncio.run(self.client.blob_exists(uri)))

            result = BytesIO()
            asyncio.run(self.client.read(uri, result))
            result.seek(0)
            self.assertEqual(b"str", result.read())

            size = asyncio.run(self.client.size_in_bytes(uri))
            self.assertEqual(3, size)

    def test_copy(self):
        with tempfile.NamedTemporaryFile("w") as from_file:
            uri_from = "file://" + from_file.name
            asyncio.run(self.client.write(uri_from, BytesIO(b"str")))
            from_file.flush()

            with tempfile.NamedTemporaryFile("w") as to_file:
                uri_to = "file://" + to_file.name
                asyncio.run(self.client.copy(uri_from, uri_to))

                result = BytesIO()
                asyncio.run(self.client.read(uri_to, result))
                result.seek(0)
                self.assertEqual(b"str", result.read())
