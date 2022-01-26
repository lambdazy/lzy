import dataclasses
from unittest import TestCase

from lzy.api import LzyLocalEnv


@dataclasses.dataclass
class WB:
    a: int
    b: int


class WhiteboardTests(TestCase):
    def test_many_wb(self):
        wb = WB(1, 1)
        with LzyLocalEnv(whiteboard=wb):
            pass
        with LzyLocalEnv(whiteboard=wb):
            pass
