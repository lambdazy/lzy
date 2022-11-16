import tempfile
from typing import Type
from unittest import TestCase

from lzy.serialization.api import Schema, StandardDataFormats, StandardSchemaFormats
from lzy.serialization.registry import DefaultSerializerRegistry
from tests.serialization.utils import serialized_and_deserialized


class PrimitiveSerializationTests(TestCase):
    def setUp(self):
        self.registry = DefaultSerializerRegistry()

    def test_primitive_serialization(self):
        var = 10
        self.assertEqual(var, serialized_and_deserialized(self.registry, var))

        var = 0.0001
        self.assertEqual(var, serialized_and_deserialized(self.registry, var))

        var = "str"
        self.assertEqual(var, serialized_and_deserialized(self.registry, var))

        var = True
        self.assertEqual(var, serialized_and_deserialized(self.registry, var))

    def test_primitive_schema(self):
        var = 10
        serializer = self.registry.find_serializer_by_type(type(var))
        with tempfile.TemporaryFile() as file:
            serializer.serialize(var, file)
            file.flush()
            file.seek(0)

            schema = serializer.schema(type(var))
            deserializer = self.registry.find_serializer_by_data_format(
                schema.data_format
            )
            typ: Type = deserializer.resolve(schema)
            # noinspection PyTypeChecker
            deserialized = deserializer.deserialize(file, typ)

        self.assertEqual(var, deserialized)
        self.assertTrue(serializer.stable())
        self.assertTrue(serializer.available())
        self.assertEqual(StandardDataFormats.primitive_type.name, schema.data_format)
        self.assertEqual(
            StandardSchemaFormats.json_pickled_type.name, schema.schema_format
        )

        with self.assertRaisesRegex(ValueError, "Invalid data format*"):
            serializer.resolve(
                Schema(
                    StandardDataFormats.proto.name,
                    StandardSchemaFormats.json_pickled_type.name,
                    "content"
                )
            )
        with self.assertRaisesRegex(ValueError, "PrimitiveSerializer supports only jsonpickle schema format*"):
            serializer.resolve(
                Schema(
                    StandardDataFormats.primitive_type.name,
                    StandardSchemaFormats.pickled_type.name,
                    "content"
                )
            )
