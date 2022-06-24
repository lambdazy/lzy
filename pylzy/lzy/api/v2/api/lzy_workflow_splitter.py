from typing import List

from lzy.api.v2.api import LzyCall
from lzy.api.v2.api.graph import Graph, GraphBuilder


class LzyWorkflowSplitter:
    def __init__(self):
        self._calls: List[LzyCall] = []

    def call(self, call: LzyCall) -> None:
        self._calls.append(call)

    def barrier(self) -> Graph:
        graph_builder = GraphBuilder()
        for call in self._calls:
            graph_builder.add_call(call)
        self._calls = []
        return graph_builder.build()
