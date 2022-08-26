import tempfile
import uuid
from dataclasses import dataclass
from typing import Any, BinaryIO, Callable, Type, Union
from unittest import TestCase

import cloudpickle

# noinspection PyPackageRequirements
from catboost import Pool
from pure_protobuf.dataclasses_ import field, message
from pure_protobuf.types import int32

from lzy.serialization.api import Serializer
from lzy.serialization.catboost import CatboostPoolSerializer
from lzy.serialization.registry import DefaultSerializersRegistry
from lzy.serialization.types import File


def generate_serializer(
        supported_types: Union[Type, Callable[[Type], bool]] = lambda x: True,
        available: bool = True,
        stable: bool = True,
) -> Type[Serializer]:
    class TestSerializer(Serializer):
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
        self.registry = DefaultSerializersRegistry()

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

        serializer_for_all = generate_serializer(available=True, supported_types=lambda x: True)()
        self.registry.register_serializer(str(uuid.uuid4()), serializer_for_all)
        self.assertEqual(self.registry.find_serializer_by_type(A), serializer_for_type)

        self.registry.unregister_serializer(name_for_type)
        self.assertEqual(self.registry.find_serializer_by_type(A), serializer_for_all)

    def test_priorities(self):
        serializer_for_type_priority_1 = generate_serializer(available=True, supported_types=lambda x: True)()
        self.registry.register_serializer(str(uuid.uuid4()), serializer_for_type_priority_1, priority=1)
        self.assertEqual(self.registry.find_serializer_by_type(A), serializer_for_type_priority_1)

        priority_0_name = str(uuid.uuid4())
        serializer_for_type_priority_0 = generate_serializer(available=True, supported_types=lambda x: True)()
        self.registry.register_serializer(priority_0_name, serializer_for_type_priority_0, priority=0)
        self.assertEqual(self.registry.find_serializer_by_type(A), serializer_for_type_priority_0)

        self.registry.unregister_serializer(priority_0_name)
        self.assertEqual(self.registry.find_serializer_by_type(A), serializer_for_type_priority_1)

    def test_filters(self):
        class Accepting:
            pass

        serializer = generate_serializer(available=True, supported_types=lambda x: "Accepting" in x.__name__, )()
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

    def test_resolve_serializer_name(self):
        serializer = self.registry.find_serializer_by_type(A)
        name = self.registry.resolve_name(serializer)
        self.assertEqual("LZY_CLOUDPICKLE_SERIALIZER", name)

        unused = generate_serializer()()
        self.assertIsNone(self.registry.resolve_name(unused))
