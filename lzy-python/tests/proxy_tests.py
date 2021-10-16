from typing import Any
from unittest import TestCase

# noinspection PyProtectedMember
from api import lazy_op_proxy, LzyOp, islazyproxy
from lzy.api._proxy import proxy


class ProxyTests(TestCase):
    def test_custom_object_with_static_and_class_methods(self):
        class A:
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

        class LazyOpMock(LzyOp):
            def __init__(self):
                super().__init__(lambda: None, (), None)

            def materialize(self) -> Any:
                a.append("Materialized without any fcking reason")
                return "AAA"

        prxy = lazy_op_proxy(LazyOpMock(), str)
        prxy._op
        islazyproxy(prxy)
        self.assertEqual(len(a), 0)
