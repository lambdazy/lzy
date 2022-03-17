import unittest

from typing import Tuple

import dataclasses
from unittest import TestCase

from lzy.api import LzyLocalEnv, op
from lzy.api.whiteboard import whiteboard
import uuid


@dataclasses.dataclass
@whiteboard(namespace='wb', tags=["simple_whiteboard"])
class WB:
    a: int
    b: int
    c: int = 3


WORKFLOW_NAME = "workflow_" + str(uuid.uuid4())


class WhiteboardTests(TestCase):
    def setUp(self) -> None:
        self._raising_num_run_num = 0

    @op
    def num(self) -> int:
        return 5

    @op
    def nums(self, c: int) -> Tuple[int, int]:
        print(c)  # Materialize c if it is proxy
        return 42, 42

    @op
    def raising_num(self) -> int:
        self._raising_num_run_num += 1
        raise Exception("Some exception")

    def test_many_wb(self):
        wb = WB(1, 1)
        with LzyLocalEnv().workflow(name=WORKFLOW_NAME, whiteboard=wb):
            wb.a = self.num()
            wb.b = self.num()
        self.assertEqual(5, wb.a)
        self.assertEqual(5, wb.b)

    def test_multiple_instances(self):
        wb = WB(1, 1)
        with LzyLocalEnv().workflow(name=WORKFLOW_NAME, whiteboard=wb):
            wb.a = self.num()

        wb2 = WB(1, 1)
        with LzyLocalEnv().workflow(name=WORKFLOW_NAME, whiteboard=wb):
            wb2.b = self.num()

        self.assertEqual(5, wb.a)
        self.assertEqual(5, wb2.b)

    def test_non_op_assign(self):
        wb = WB(1, 1)
        with LzyLocalEnv().workflow(name=WORKFLOW_NAME, whiteboard=wb):
            wb.a = self.num()
            wb.b = 5
        self.assertEqual(5, wb.b)

    def test_multiple_assigns(self):
        with self.assertRaises(ValueError) as context:
            wb = WB(1, 1)
            with LzyLocalEnv().workflow(name=WORKFLOW_NAME, whiteboard=wb):
                wb.a = 3
                wb.a = self.num()
            self.assertTrue('Whiteboard field can be assigned only once' in str(context.exception))

    def test_default_assigns(self):
        with self.assertRaises(ValueError) as context:
            wb = WB(1, 1)
            with LzyLocalEnv().workflow(name=WORKFLOW_NAME, whiteboard=wb):
                wb.a = self.num()
                wb.a = self.num()
        self.assertTrue('Whiteboard field can be assigned only once' in str(context.exception))

    @unittest.skip
    def test_execution_stop_if_whiteboard_invalid(self):
        wb = WB(1, 1)
        with self.assertRaises(ValueError) as context:
            with LzyLocalEnv().workflow(name=WORKFLOW_NAME, whiteboard=wb):
                wb.a = self.num()
                wb.b = 5

        self.assertEqual(1, wb.a)
        self.assertTrue('Only @op return values can be assigned to whiteboard' in str(context.exception))

    def test_materialization_stops_if_whiteboard_invalid(self):
        with self.assertRaises(Exception):
            with LzyLocalEnv().workflow(name=WORKFLOW_NAME):
                a = self.raising_num()
                b = self.nums(a)
                c, d = b
                print(c, d)

        self.assertEqual(1, self._raising_num_run_num)
