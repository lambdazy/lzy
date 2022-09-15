import uuid
from typing import Any, BinaryIO, Callable, Dict, Type, Union
from unittest import TestCase

from lzy.serialization.api import Serializer
from lzy.serialization.registry import DefaultSerializerRegistry


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


class SerializationRegistryTests(TestCase):
    def setUp(self):
        self.registry = DefaultSerializerRegistry()

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

    def test_resolve_serializer_name(self):
        serializer = self.registry.find_serializer_by_type(A)
        name = self.registry.resolve_name(serializer)
        self.assertEqual("LZY_CLOUDPICKLE_SERIALIZER", name)

        unused = generate_serializer()()
        self.assertIsNone(self.registry.resolve_name(unused))
