import unittest

from typing import Tuple

import dataclasses
from unittest import TestCase

from lzy.api import LzyLocalEnv, op
from lzy.api.whiteboard import whiteboard


@dataclasses.dataclass
@whiteboard(namespace='wb', tags=["simple_whiteboard"])
class WB:
    a: int
    b: int
    c: int = 3


class WhiteboardTests(TestCase):
    def setUp(self) -> None:
        self._num_run_num = 0

    @op
    def num(self) -> int:
        self._num_run_num += 1
        return 5

    @op
    def nums(self, c: int) -> Tuple[int, int]:
        print(c)
        return 5, 5

    def test_many_wb(self):
        wb = WB(1, 1)
        with LzyLocalEnv(whiteboard=wb):
            wb.a = self.num()
            wb.b = self.num()
        self.assertEqual(5, wb.a)
        self.assertEqual(5, wb.b)

    def test_multiple_instances(self):
        wb = WB(1, 1)
        with LzyLocalEnv(whiteboard=wb):
            wb.a = self.num()

        wb2 = WB(1, 1)
        with LzyLocalEnv(whiteboard=wb):
            wb2.b = self.num()

        self.assertEqual(5, wb.a)
        self.assertEqual(5, wb2.b)

    def test_non_op_assign(self):
        with self.assertRaises(ValueError) as context:
            wb = WB(1, 1)
            with LzyLocalEnv(whiteboard=wb):
                wb.a = self.num()
                wb.b = 5
        self.assertTrue('Only @op return values can be assigned to whiteboard' in str(context.exception))

    def test_multiple_assigns(self):
        with self.assertRaises(ValueError) as context:
            wb = WB(1, 1)
            with LzyLocalEnv(whiteboard=wb):
                wb.a = self.num()
                wb.a = self.num()
        self.assertTrue('Whiteboard field can be assigned only once' in str(context.exception))

    @unittest.skip
    def test_execution_stop_if_whiteboard_invalid(self):
        wb = WB(1, 1)
        with self.assertRaises(ValueError) as context:
            with LzyLocalEnv(whiteboard=wb):
                wb.a = self.num()
                wb.b = 5

        self.assertEqual(1, wb.a)
        self.assertTrue('Only @op return values can be assigned to whiteboard' in str(context.exception))

    @unittest.skip
    def test_materialization_stops_if_whiteboard_invalid(self):
        wb = WB(1, 1)
        with self.assertRaises(ValueError) as context:
            with LzyLocalEnv(whiteboard=wb):
                wb.a = self.num()
                self.num()
                wb.b, wb.c = self.nums(wb.a)

        self.assertEqual(0, self._num_run_num)
        self.assertTrue('Only @op return values can be assigned to whiteboard' in str(context.exception))
