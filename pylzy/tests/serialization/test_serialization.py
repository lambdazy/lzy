import tempfile
from dataclasses import dataclass
from unittest import TestCase

import cloudpickle
from pure_protobuf.dataclasses_ import message, field
from pure_protobuf.types import int32

from lzy.serialization.serializer import DefaultSerializer


@message
@dataclass
class TestMessage:
    a: int32 = field(1, default=0)


class SerializationTests(TestCase):
    @classmethod
    def setUpClass(cls):
        cls.serialization = DefaultSerializer()

    def test_unpickled_message_keeps_subclass(self):
        msg = TestMessage(42)
        pickled_msg_type = cloudpickle.dumps(type(msg))
        unpickled_msg_type = cloudpickle.loads(pickled_msg_type)

        with tempfile.TemporaryFile() as file:
            self.serialization.serialize(msg, file)
            file.flush()
            file.seek(0)
            result = self.serialization.deserialize(file, unpickled_msg_type)

        self.assertEqual(msg.a, result.a)
