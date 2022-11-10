import tempfile
from dataclasses import dataclass
from unittest import TestCase

import cloudpickle
from pure_protobuf.dataclasses_ import field, message
from pure_protobuf.types import int32

from lzy.serialization.api import Schema, StandardDataFormats, StandardSchemaFormats
from lzy.serialization.registry import DefaultSerializerRegistry


class CatboostSerializationTests(TestCase):
    def setUp(self):
        self.registry = DefaultSerializerRegistry()

    def test_cloudpickle_serializer(self):
        class B:
            def __init__(self, x: int):
                self.x = x

        serializer = self.registry.find_serializer_by_type(B)
        b = B(42)

        with tempfile.TemporaryFile() as file:
            serializer.serialize(b, file)
            file.flush()
            file.seek(0)
            deserialized = serializer.deserialize(file, B)

        self.assertEqual(b.x, deserialized.x)
        self.assertFalse(serializer.stable())
        self.assertIn("cloudpickle", serializer.meta())

    def test_cloudpickle_schema(self):
        class B:
            def __init__(self, x: int):
                self.x = x

        serializer = self.registry.find_serializer_by_type(B)
        b = B(42)
        schema = serializer.schema(type(b))

        self.assertEqual(StandardDataFormats.pickle.name, schema.data_format)
        self.assertEqual(StandardSchemaFormats.pickled_type.name, schema.schema_format)

        deserializer = self.registry.find_serializer_by_data_format(schema.data_format)
        with tempfile.TemporaryFile() as file:
            serializer.serialize(b, file)
            file.flush()
            file.seek(0)
            deserialized = deserializer.deserialize(file, B)
        self.assertEqual(b.x, deserialized.x)

        typ = deserializer.resolve(schema)
        self.assertEqual(B.__module__, typ.__module__)
        self.assertEqual(B.__name__, typ.__name__)

        with self.assertRaisesRegex(ValueError, "Invalid data format*"):
            serializer.resolve(
                Schema(
                    StandardDataFormats.proto.name,
                    StandardSchemaFormats.pickled_type.name,
                )
            )
        with self.assertRaisesRegex(ValueError, "Invalid schema format*"):
            serializer.resolve(
                Schema(
                    StandardDataFormats.pickle.name,
                    StandardSchemaFormats.json_pickled_type.name,
                )
            )
        with self.assertRaisesRegex(ValueError, "No schema content*"):
            serializer.resolve(
                Schema(
                    StandardDataFormats.pickle.name,
                    StandardSchemaFormats.pickled_type.name,
                )
            )

    def test_unpickled_message_keeps_subclass(self):
        @message
        @dataclass
        class TestMessage:
            a: int32 = field(1, default=0)

        msg = TestMessage(42)
        pickled_msg_type = cloudpickle.dumps(type(msg))
        unpickled_msg_type = cloudpickle.loads(pickled_msg_type)

        with tempfile.TemporaryFile() as file:
            self.registry.find_serializer_by_type(type(msg)).serialize(msg, file)
            file.flush()
            file.seek(0)
            result = self.registry.find_serializer_by_type(
                unpickled_msg_type
            ).deserialize(file, unpickled_msg_type)

        self.assertEqual(msg.a, result.a)
