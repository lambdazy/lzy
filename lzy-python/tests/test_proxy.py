from abc import ABC
from typing import Any, Optional
from unittest import TestCase

from lzy.api import LzyOp

from lzy.api.utils import lazy_proxy, is_lazy_proxy
# noinspection PyProtectedMember
from lzy.api._proxy import proxy


class ProxyTests(TestCase):
    def test_custom_object_with_static_and_class_methods(self):
        class A:
            # noinspection PyShadowingNames
            def __init__(self, a, b, c):
                self.__a = a
                self._b = b
                self.c = c

            @property
            def a(self):
                return self.__a

            @a.setter
            def a(self, value):
                self.__a = value

            def f(self):
                return self.c

            @classmethod
            def class_method(cls):
                return cls

            @staticmethod
            def kung(oh, way):
                return oh + way

        a = proxy(lambda: A(4, 1, 3), A)
        self.assertEqual(4, a.a)
        self.assertEqual(1, a._b)
        self.assertEqual(3, a.f())

        a.l = 550
        self.assertEqual(550, a.l)
        a.l = 5
        self.assertEqual(5, a.l)

        a.a = 55
        self.assertEqual(55, a.a)

        expected = "oh hohoho"
        self.assertEqual(expected, a.kung(expected[:4], expected[4:]))
        self.assertIs(a.class_method(), A)

    def test_primitive_type(self):
        integer = proxy(lambda: 42, int)
        val = proxy(lambda: 0, int)
        for i in range(integer):
            val += 1
        self.assertEqual(42, val)

        string_ = proxy(lambda: "string   ", str)
        expected = "string"
        self.assertEqual(string_.rstrip(), expected)

        res = 0
        b = proxy(lambda: True, bool)
        if b:
            res = 1
        self.assertEqual(res, 1)

        lst = proxy(lambda: [1, 2, 3, 4, 5], list)
        self.assertEqual(len(lst), 5)

    def test_slots(self):
        class B:
            __slots__ = ('_fields',)

            def __init__(self, _fields=None):
                if not _fields:
                    self._fields = set()

        a = proxy(B, B)
        self.assertIsInstance(a._fields, set)
        a._fields = None
        self.assertIs(a._fields, None)
        self.assertTupleEqual(B.__slots__, a.__slots__)

    def test_lazy(self):
        a = []

        class LazyOpMock(LzyOp, ABC):
            def __init__(self):
                super().__init__(lambda: None, (), None, ())

            def materialize(self) -> Any:
                a.append("Materialized without any fcking reason")
                return "AAA"

            def is_materialized(self) -> bool:
                return False

            def return_entry_id(self) -> Optional[str]:
                return None

        mock = LazyOpMock()
        prxy = lazy_proxy(lambda: mock.materialize(), str, {'_op': mock})
        op = prxy._op
        is_lazy_proxy(prxy)
        self.assertEqual(len(a), 0)
        op.materialize()
        self.assertEqual(len(a), 1)

    def test_int_sum(self):
        a = 10
        b = 50

        prxy_a = proxy(lambda: a, int)
        prxy_b = proxy(lambda: b, int)
        self.assertEqual(prxy_a + prxy_b, 60)
        self.assertEqual(prxy_a * prxy_b, 500)
