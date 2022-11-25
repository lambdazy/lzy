import tempfile
from unittest import TestCase

from lzy.serialization.lzy_serializer_registry import LzySerializerRegistry
from lzy.types import File


class FileSerializationTests(TestCase):
    def setUp(self):
        self.registry = LzySerializerRegistry()

    def test_file_serializer(self):
        content = "test string"
        with tempfile.NamedTemporaryFile() as source:
            source.write(content.encode())
            source.flush()

            file = File(source.name)
            serializer = self.registry.find_serializer_by_type(File)
            with tempfile.TemporaryFile() as tmp:
                serializer.serialize(file, tmp)
                tmp.flush()
                tmp.seek(0)
                deserialized_file = serializer.deserialize(tmp, File)

        with deserialized_file.open() as file:
            self.assertEqual(content, file.read())
        self.assertTrue(serializer.stable())
        self.assertIn("pylzy", serializer.meta())
