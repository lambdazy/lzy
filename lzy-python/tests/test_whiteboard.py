import dataclasses
from unittest import TestCase

from lzy.api import LzyLocalEnv, op
from lzy.api.whiteboard import whiteboard


@dataclasses.dataclass
@whiteboard(namespace='wb', tags=["simple_whiteboard"])
class WB:
    a: int
    b: int


@op
def num() -> int:
    return 5


class WhiteboardTests(TestCase):
    def test_many_wb(self):
        wb = WB(1, 1)
        with LzyLocalEnv(whiteboard=wb):
            wb.a = num()
            wb.b = num()
        self.assertEqual(5, wb.a)
        self.assertEqual(5, wb.b)

    def test_multiple_instances(self):
        wb = WB(1, 1)
        with LzyLocalEnv(whiteboard=wb):
            wb.a = num()

        wb2 = WB(1, 1)
        with LzyLocalEnv(whiteboard=wb):
            wb2.b = num()

        self.assertEqual(5, wb.a)
        self.assertEqual(5, wb2.b)

    def test_non_op_assign(self):
        with self.assertRaises(ValueError) as context:
            wb = WB(1, 1)
            with LzyLocalEnv(whiteboard=wb):
                wb.a = num()
                wb.b = 5
        self.assertTrue('Only @op return values can be assigned to whiteboard' in str(context.exception))

    def test_multiple_assigns(self):
        with self.assertRaises(ValueError) as context:
            wb = WB(1, 1)
            with LzyLocalEnv(whiteboard=wb):
                wb.a = num()
                wb.a = num()
        self.assertTrue('Whiteboard field can be assigned only once' in str(context.exception))
