from typing import Optional, Union, List
from unittest import TestCase

from lzy.api.v1.utils.types import infer_real_types


class TypesTests(TestCase):
    def test_infer_primitive(self):
        self.assertEqual((int,), infer_real_types(int))
        self.assertEqual((str,), infer_real_types(str))
        self.assertEqual((float,), infer_real_types(float))

    def test_infer_list(self):
        self.assertEqual((list,), infer_real_types(List[str]))
        self.assertEqual((list,), infer_real_types(List[int]))

    def test_infer_optional(self):
        typ = infer_real_types(Optional[int])
        self.assertEqual([int, type(None)], typ)

    def test_infer_union(self):
        typ = infer_real_types(Union[str, Union[int, Union[float, type(None)]]])
        self.assertEqual([str, int, float, type(None)], typ)
