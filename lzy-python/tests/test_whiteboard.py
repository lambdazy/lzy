import dataclasses
from unittest import TestCase

import lzy


@dataclasses.dataclass
class WB:
    a: int
    b: int

class WhiteboardTests(TestCase):
    def test_many_wb(self):
        wb = WB(1, 1)
        with lzy.api.LzyEnv(local=True, whiteboard=wb):
            pass
        with lzy.api.LzyEnv(local=True, whiteboard=wb):
            pass
