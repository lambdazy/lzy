from typing import List, Optional

from lzy.v2.api import LzyCall
from lzy.v2.api.graph import GraphBuilder, Graph


class LzyWorkflowSplitter:
    def __init__(self):
        self._calls: List[LzyCall] = []

    def call(self, call: LzyCall) -> None:
        self._calls.append(call)

    def barrier(self, call: Optional[LzyCall] = None) -> Graph:
        graph_builder = GraphBuilder()
        if call is None:
            for call in self._calls:
                graph_builder.add_call(call)
            self._calls = []
        else:
            if not call.executed:
                graph_builder.add_call(call)
        return graph_builder.build()

