import dataclasses
from unittest import TestCase

from lzy.api import LzyEnv


@dataclasses.dataclass
class WB:
    a: int
    b: int


class WhiteboardTests(TestCase):
    def test_many_wb(self):
        wb = WB(1, 1)
        with LzyEnv(local=True, whiteboard=wb):
            pass
        with LzyEnv(local=True, whiteboard=wb):
            pass
