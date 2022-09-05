from unittest import TestCase

from lzy.serialization.hasher import DelegatingHasher
from lzy.serialization.registry import DefaultSerializerRegistry


class A:
    pass


class HasherTests(TestCase):
    @classmethod
    def setUpClass(cls):
        cls.hasher = DelegatingHasher(DefaultSerializerRegistry())

    def test_simple_object(self):
        obj = "str"

        can_hash = self.hasher.can_hash(obj)
        value = self.hasher.hash(obj)

        self.assertTrue(can_hash)
        self.assertEqual(value, "eddf6e1147a82679c4fbbf85d0ff2d48")

    def test_complex_object(self):
        obj = A()

        can_hash = self.hasher.can_hash(obj)
        value = self.hasher.hash(obj)

        self.assertTrue(can_hash)
        self.assertEqual(value, "02dc469d361fc32593c6888e80ed86f4")
