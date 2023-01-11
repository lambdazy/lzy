from abc import ABC
from dataclasses import dataclass
from typing import Any, List
from unittest import TestCase

from lzy.api.v1.utils.types import infer_real_type
from lzy.proxy import proxy


class ProxyTests(TestCase):
    @staticmethod
    def lazy_constructed_obj(cls, *args, **kwargs):
        return proxy(lambda: cls(*args, **kwargs), cls)

    def test_simple_isinstance(self):
        class A:
            pass

        prxy_ = self.lazy_constructed_obj(A)
        self.assertIsInstance(prxy_, A)

        prxy_int_ = self.lazy_constructed_obj(int)
        self.assertIsInstance(prxy_int_, int)

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

        a = self.lazy_constructed_obj(A, 4, 1, 3)
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
        integer = self.lazy_constructed_obj(int, 42)
        val = self.lazy_constructed_obj(int, 0)
        for i in range(integer):
            val += 1
        self.assertEqual(42, val)

        # useless but good for tests
        string_ = self.lazy_constructed_obj(str, "string   ")
        expected = "string"
        self.assertEqual(string_.rstrip(), expected)

        res = 0
        b = self.lazy_constructed_obj(bool, True)
        if b:
            res = 1
        self.assertEqual(res, 1)

        lst = self.lazy_constructed_obj(list, [1, 2, 3, 4, 5])
        self.assertEqual(len(lst), 5)

    def test_slots(self):
        class B:
            __slots__ = ("_fields",)

            def __init__(self, _fields=None):
                if not _fields:
                    self._fields = set()

        a = self.lazy_constructed_obj(B)
        self.assertIsInstance(a._fields, set)
        a._fields = None
        self.assertIs(a._fields, None)
        self.assertTupleEqual(B.__slots__, a.__slots__)

    def test_int_sum(self):
        a = 10
        b = 50

        prxy_a = self.lazy_constructed_obj(int, a)
        prxy_b = self.lazy_constructed_obj(int, b)
        self.assertEqual(prxy_a + prxy_b, 60)
        self.assertEqual(prxy_a * prxy_b, 500)

    def test_real_descriptor(self):
        class Field:
            def __init__(self, value):
                self.value = value

            def __get__(self, instance, owner):
                return self.value

            def __eq__(self, other):
                return other.value == self.value

        class IntField(Field):
            def __init__(self, value=0):
                super().__init__(value)

        class MockType:
            a = IntField(41)
            b = IntField(42)

        materialized = []
        prxy = proxy(lambda: materialized.append(1) or MockType(), MockType)
        self.assertNotEqual(prxy.a, prxy.b)
        self.assertEqual(len(materialized), 1)

    def test_proxy_set(self):
        class Field:
            def __init__(self, value):
                self.value = value

            def __get__(self, instance, owner):
                return self.value

            def __set__(self, instance, value):
                self.value = value

            def __eq__(self, other):
                return other.value == self.value

        class IntField(Field):
            def __init__(self, value=0):
                super().__init__(value)

        class MockType:
            a = IntField(41)
            b = IntField(42)

        materialized = []
        prxy = proxy(lambda: materialized.append(1) or MockType(), MockType)
        prxy.a = 42
        self.assertEqual(len(materialized), 1)
        self.assertEqual(prxy.a, prxy.b)

    def test_exception(self):
        class CrazyException(Exception):
            pass

        class CrazyClass:
            def crazy_func(self):
                raise CrazyException("Ahaa")

        with self.assertRaises(CrazyException):
            prxy: CrazyClass = proxy(lambda: CrazyClass(), CrazyClass)
            prxy.crazy_func()

    def test_lists(self):
        @dataclass
        class A:
            val: int

        a = [A(0), A(1), A(2)]
        prxy_a = proxy(lambda: a, infer_real_type(List[A]))
        self.assertEqual(prxy_a[0].val, 0)
        self.assertEqual(prxy_a[1].val, 1)
        self.assertEqual(prxy_a[2].val, 2)

    def test_proxy_with_custom_new(self):
        class ClassWithCustomNew:
            def __new__(cls):
                return super().__new__(cls)

            def __init__(self):
                self.test_attr = 42

        prxy = proxy(ClassWithCustomNew, ClassWithCustomNew)
        self.assertEqual(prxy.test_attr, 42)

    def test_origin(self):
        integer = self.lazy_constructed_obj(int, 42)
        self.assertNotEqual(int, type(integer))
        self.assertEqual(int, type(integer.__lzy_origin__))
