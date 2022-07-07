from typing import Any, Dict, Iterator, List, Set

from lzy.api.v2.api import LzyCall
from lzy.api.v2.utils import is_lazy_proxy, materialized


class BuildError(Exception):
    pass


def unique_values(values: List[Any]):
    seen: Set[Any] = set()
    seen_add = seen.add
    return [x for x in values if not (x in seen or seen_add(x))]


class Graph:
    def __init__(
        self,
        snapshot_id: str,
        calls: List[LzyCall],
        id_to_call: Dict[str, LzyCall],
        adjacency_graph: Dict[str, List[str]],
    ):
        self._snapshot_id = snapshot_id
        self._calls = unique_values(calls)
        self._id_to_call = id_to_call
        self._adjacency_graph = adjacency_graph

    def snapshot_id(self) -> str:
        return self._snapshot_id

    def calls(self) -> Iterator[LzyCall]:
        return self._calls.__iter__()  # type: ignore

    def adjacent(self, call: LzyCall) -> Iterator[LzyCall]:
        for _call_id in self._adjacency_graph[call.id]:
            yield self._id_to_call[_call_id]


class GraphBuilder:
    def __init__(self):
        self._snapshot_id = None
        self._calls = []
        self._id_to_call = {}
        self._adjacency_graph = {}
        self._already_built = False

    def snapshot_id(self, snapshot_id: str) -> "GraphBuilder":
        if self._already_built:
            raise BuildError(
                "Setting snapshot_id in an already built graph is not allowed"
            )
        self._snapshot_id = snapshot_id
        return self

    def _contains_call(self, call: LzyCall):
        for _call in self._calls:
            if _call.id == call.id:
                return True
        return False

    def _dependent_calls(self, call: LzyCall):
        dependent_calls: List[LzyCall] = []
        for name, arg in call.named_arguments():
            if is_lazy_proxy(arg) and not materialized(arg):
                dependent_calls.extend(self._dependent_calls(arg.lzy_call))
                dependent_calls.append(arg.lzy_call)
        return unique_values(dependent_calls)

    def add_call(self, call: LzyCall) -> "GraphBuilder":
        if self._already_built:
            raise BuildError("Adding call to an already built graph is not allowed")
        dependent_calls: List[LzyCall] = self._dependent_calls(call)
        for dependent_call in dependent_calls:
            if not self._contains_call(dependent_call):
                self._calls.append(dependent_call)
                self._id_to_call[dependent_call.id] = dependent_call
            if dependent_call.id not in self._adjacency_graph:
                self._adjacency_graph[dependent_call.id] = []
            self._adjacency_graph[dependent_call.id].append(call.id)
        self._calls.append(call)
        self._id_to_call[call.id] = call
        return self

    def build(self) -> Graph:
        self._already_built = True
        return Graph(
            snapshot_id=self._snapshot_id,
            calls=self._calls,
            id_to_call=self._id_to_call,
            adjacency_graph=self._adjacency_graph,
        )
