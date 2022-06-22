from dataclasses import dataclass


from unittest import TestCase

from pure_protobuf.dataclasses_ import message, one_of, part
from pure_protobuf.oneof import OneOf_


class TestOneOf(TestCase):
    def test_simple_isinstance(self):
        @message
        @dataclass
        class OneOfTest:
            # fmt: off
            a: OneOf_ = one_of(
                b=part(int, 1),
                c=part(bool, 2)
            )
            # fmt: on

        c = OneOfTest()
        c.a.b = 5
        self.assertEqual(c.a.which_one_of, "b")
        self.assertEqual(c.a.b, 5)
        self.assertIsNone(c.a.c)
