import tempfile
from unittest import TestCase

from serialzy.api import StandardDataFormats, StandardSchemaFormats, Schema

from lzy.serialization.registry import LzySerializerRegistry
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

    def test_schema(self):
        serializer = self.registry.find_serializer_by_data_format("lzy_raw_file")
        schema = serializer.schema(File)

        self.assertEqual("lzy_raw_file", schema.data_format)
        self.assertEqual(StandardSchemaFormats.no_schema.name, schema.schema_format)
        self.assertEqual('', schema.schema_content)
        self.assertIn("pylzy", schema.meta)

        with self.assertRaisesRegex(ValueError, 'Invalid type*'):
            serializer.schema(str)

    def test_resolve(self):
        serializer = self.registry.find_serializer_by_data_format("lzy_raw_file")
        typ = serializer.resolve(Schema("lzy_raw_file", StandardSchemaFormats.no_schema.name))
        self.assertEqual(File, typ)

        with self.assertRaisesRegex(ValueError, 'Invalid data format*'):
            serializer.resolve(
                Schema(StandardDataFormats.proto.name, StandardSchemaFormats.no_schema.name, meta={'pylzy': '0.0.0'}))

        with self.assertRaisesRegex(ValueError, 'Invalid schema format*'):
            serializer.resolve(
                Schema("lzy_raw_file", StandardSchemaFormats.pickled_type.name,
                       meta={'pylzy': '0.0.0'}))

        with self.assertLogs() as cm:
            serializer.resolve(
                Schema("lzy_raw_file", StandardSchemaFormats.no_schema.name))
            self.assertRegex(cm.output[0], 'WARNING:lzy.serialization.file:No pylzy version in meta*')

        with self.assertLogs() as cm:
            serializer.resolve(
                Schema("lzy_raw_file", StandardSchemaFormats.no_schema.name,
                       meta={'pylzy': '100000.0.0'}))
            self.assertRegex(cm.output[0], 'WARNING:lzy.serialization.file:Installed version of pylzy*')
