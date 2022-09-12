import tempfile
import uuid
from dataclasses import dataclass
from typing import Any, BinaryIO, Callable, Dict, Type, Union
from unittest import TestCase

import cloudpickle

# noinspection PyPackageRequirements
from catboost import Pool
from pure_protobuf.dataclasses_ import field, message
from pure_protobuf.types import int32

from lzy.serialization.api import (
    Schema,
    Serializer,
    StandardDataFormats,
    StandardSchemaFormats,
)
from lzy.serialization.catboost import CatboostPoolSerializer
from lzy.serialization.registry import DefaultSerializerRegistry
from lzy.serialization.types import File


def generate_serializer(
    supported_types: Union[Type, Callable[[Type], bool]] = lambda x: True,
    available: bool = True,
    stable: bool = True,
) -> Type[Serializer]:
    class TestSerializer(Serializer):
        def format(self) -> str:
            return "test_format"

        def meta(self) -> Dict[str, str]:
            return {}

        def serialize(self, obj: Any, dest: BinaryIO) -> None:
            pass

        def deserialize(self, source: BinaryIO, typ: Type) -> Any:
            pass

        def stable(self) -> bool:
            return stable

        def available(self) -> bool:
            return available

        def supported_types(self) -> Union[Type, Callable[[Type], bool]]:
            return supported_types

    return TestSerializer


class A:
    pass


class SerializationTests(TestCase):
    def setUp(self):
        self.registry = DefaultSerializerRegistry()

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

    def test_catboost_pool_serialization(self):
        pool = Pool(
            data=[[1, 4, 5, 6], [4, 5, 6, 7], [30, 40, 50, 60]],
            label=[1, 1, -1],
            weight=[0.1, 0.2, 0.3],
        )
        serializer = self.registry.find_serializer_by_type(type(pool))
        with tempfile.TemporaryFile() as file:
            serializer.serialize(pool, file)
            file.flush()
            file.seek(0)
            deserialized_pool = serializer.deserialize(file, Pool)

        self.assertTrue(isinstance(serializer, CatboostPoolSerializer))
        self.assertEqual(pool.get_weight(), deserialized_pool.get_weight())
        self.assertTrue(serializer.stable())
        self.assertIn("catboost_version", serializer.meta())

    def test_register_unregister_serializer_for_type(self):
        serializer = generate_serializer(available=True, supported_types=A)()
        name = str(uuid.uuid4())
        self.registry.register_serializer(name, serializer)
        self.assertEqual(self.registry.find_serializer_by_type(A), serializer)

        self.registry.unregister_serializer(name)
        self.assertNotEqual(self.registry.find_serializer_by_type(A), serializer)

    def test_register_unavailable_serializer(self):
        serializer = generate_serializer(available=False, supported_types=A)()
        self.registry.register_serializer("UNAVAILABLE", serializer)
        self.assertNotEqual(self.registry.find_serializer_by_type(A), serializer)

    def test_serializer_for_type_prioritized(self):
        serializer_for_type = generate_serializer(available=True, supported_types=A)()
        name_for_type = str(uuid.uuid4())
        self.registry.register_serializer(name_for_type, serializer_for_type)
        self.assertEqual(self.registry.find_serializer_by_type(A), serializer_for_type)

        serializer_for_all = generate_serializer(
            available=True, supported_types=lambda x: True
        )()
        self.registry.register_serializer(str(uuid.uuid4()), serializer_for_all)
        self.assertEqual(self.registry.find_serializer_by_type(A), serializer_for_type)

        self.registry.unregister_serializer(name_for_type)
        self.assertEqual(self.registry.find_serializer_by_type(A), serializer_for_all)

    def test_priorities(self):
        serializer_for_type_priority_1 = generate_serializer(
            available=True, supported_types=lambda x: True
        )()
        self.registry.register_serializer(
            str(uuid.uuid4()), serializer_for_type_priority_1, priority=1
        )
        self.assertEqual(
            self.registry.find_serializer_by_type(A), serializer_for_type_priority_1
        )

        priority_0_name = str(uuid.uuid4())
        serializer_for_type_priority_0 = generate_serializer(
            available=True, supported_types=lambda x: True
        )()
        self.registry.register_serializer(
            priority_0_name, serializer_for_type_priority_0, priority=0
        )
        self.assertEqual(
            self.registry.find_serializer_by_type(A), serializer_for_type_priority_0
        )

        self.registry.unregister_serializer(priority_0_name)
        self.assertEqual(
            self.registry.find_serializer_by_type(A), serializer_for_type_priority_1
        )

    def test_filters(self):
        class Accepting:
            pass

        serializer = generate_serializer(
            available=True,
            supported_types=lambda x: "Accepting" in x.__name__,
        )()
        self.registry.register_serializer(str(uuid.uuid4()), serializer)
        self.assertEqual(self.registry.find_serializer_by_type(Accepting), serializer)
        self.assertNotEqual(self.registry.find_serializer_by_type(A), serializer)

    def test_register_serializer_with_the_same_name(self):
        serializer_1 = generate_serializer()()
        serializer_2 = generate_serializer()()
        self.registry.register_serializer("UNUSUAL_NAME", serializer_1)
        with self.assertRaises(ValueError):
            self.registry.register_serializer("UNUSUAL_NAME", serializer_2)

    def test_register_serializer_for_the_same_type(self):
        serializer_1 = generate_serializer(supported_types=A)()
        serializer_2 = generate_serializer(supported_types=A)()
        self.registry.register_serializer(str(uuid.uuid4()), serializer_1)
        with self.assertRaises(ValueError):
            self.registry.register_serializer(str(uuid.uuid4()), serializer_2)

    def test_find_serializer_by_name(self):
        name = str(uuid.uuid4())
        serializer = generate_serializer()()
        self.registry.register_serializer(name, serializer)

        serializer_by_name = self.registry.find_serializer_by_name(name)
        self.assertEqual(serializer, serializer_by_name)
        self.assertIsNone(self.registry.find_serializer_by_name(str(uuid.uuid4())))

    def test_default_serializer(self):
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
        self.assertIn("cloudpickle_version", serializer.meta())

    def test_default_schema(self):
        class B:
            def __init__(self, x: int):
                self.x = x

        serializer = self.registry.find_serializer_by_type(B)
        b = B(42)
        schema = serializer.schema(b)

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
        self.assertIn("lzy_version", serializer.meta())

    def test_resolve_serializer_name(self):
        serializer = self.registry.find_serializer_by_type(A)
        name = self.registry.resolve_name(serializer)
        self.assertEqual("LZY_CLOUDPICKLE_SERIALIZER", name)

        unused = generate_serializer()()
        self.assertIsNone(self.registry.resolve_name(unused))

    def test_primitive_serialization(self):
        var = 10
        self.assertEqual(var, self.serialized_and_deserialized(var))

        var = 0.0001
        self.assertEqual(var, self.serialized_and_deserialized(var))

        var = "str"
        self.assertEqual(var, self.serialized_and_deserialized(var))

        var = True
        self.assertEqual(var, self.serialized_and_deserialized(var))

    def test_primitive_schema(self):
        var = 10
        serializer = self.registry.find_serializer_by_type(type(var))
        with tempfile.TemporaryFile() as file:
            serializer.serialize(var, file)
            file.flush()
            file.seek(0)

            schema = serializer.schema(var)
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
                )
            )
        with self.assertRaisesRegex(ValueError, "Invalid schema format*"):
            serializer.resolve(
                Schema(
                    StandardDataFormats.primitive_type.name,
                    StandardSchemaFormats.pickled_type.name,
                )
            )

    def test_proto_serialization(self):
        @message
        @dataclass
        class TestMessage:
            a: int32 = field(1, default=0)

        test_message = TestMessage(42)
        self.assertEqual(
            test_message.a, self.serialized_and_deserialized(test_message).a
        )

        serializer = self.registry.find_serializer_by_type(type(test_message))
        self.assertTrue(serializer.stable())
        self.assertIn("pure_protobuf_version", serializer.meta())

    def serialized_and_deserialized(self, var: Any) -> Any:
        serializer = self.registry.find_serializer_by_type(type(var))
        with tempfile.TemporaryFile() as file:
            serializer.serialize(var, file)
            file.flush()
            file.seek(0)
            deserialized = serializer.deserialize(file, type(var))
        return deserialized
