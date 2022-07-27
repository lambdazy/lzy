from dataclasses import dataclass
from typing import Callable, List

from lzy.api.v2.api.lzy_call import LzyCall
from lzy.api.v2.api.snapshot.snapshot import Snapshot


@dataclass
class ProgressStep:
    pass


class Runtime:
    def exec(
        self,
        graph: List[LzyCall],
        snapshot: Snapshot,
        progress: Callable[[ProgressStep], None],
    ) -> None:
        pass

    def destroy(self) -> None:
        pass
