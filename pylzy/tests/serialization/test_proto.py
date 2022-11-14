from dataclasses import dataclass
from typing import Optional
from unittest import TestCase

from pure_protobuf.dataclasses_ import field, message
from pure_protobuf.types import int32

from lzy.serialization.registry import DefaultSerializerRegistry
from tests.serialization.utils import serialized_and_deserialized

# noinspection PyPackageRequirements


class ProtoSerializationTests(TestCase):
    def setUp(self):
        self.registry = DefaultSerializerRegistry()

    def test_optional(self):
        serializer = self.registry.find_serializer_by_type(Optional[str])

    def test_proto_serialization(self):
        @message
        @dataclass
        class TestMessage:
            a: int32 = field(1, default=0)

        test_message = TestMessage(42)
        self.assertEqual(
            test_message.a, serialized_and_deserialized(self.registry, test_message).a
        )

        serializer = self.registry.find_serializer_by_type(type(test_message))
        self.assertTrue(serializer.stable())
        self.assertIn("hidden-pure-protobuf", serializer.meta())
