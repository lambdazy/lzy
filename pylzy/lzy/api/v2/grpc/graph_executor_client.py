from typing import List, Optional, TYPE_CHECKING, Tuple

from grpclib.client import Channel
from uuid import uuid4

from ai.lzy.v1 import SlotToChannelAssignment
from ai.lzy.v1.graph import (
    ChannelDesc,
    GraphExecutionStatus,
    GraphExecuteRequest,
    GraphListRequest,
    TaskDesc,
    GraphStatusRequest,
    GraphStopRequest,
)
from ai.lzy.v1.graph.graph_executor_grpc import GraphExecutorStub

if TYPE_CHECKING:
    from lzy.api.v2.api.lzy_call import LzyCall
    from lzy.api.v2.proxy_adapter import is_lzy_proxy


def prepare_task(call: LzyCall) -> TaskDesc:
    loc_args, non_loc_args = [], []
    for name, arg in call.named_arguments():
        slot_name = f"{call.description}:{name}"
        if is_lzy_proxy(arg):
            non_loc_args.append(slot_name)
        else:
            loc_args.append(slot_name)

    slot_assignments = [
        SlotToChannelAssignment(s_name, str(uuid4())) for s_name in non_loc_args
    ]
    return TaskDesc(
        str(uuid4()),
        call.zygote,
        slot_assignments=slot_assignments,
    )


def prepare_tasks_and_channels(
    wflow_id: str,
    tasks: List[LzyCall],
) -> Tuple[List[TaskDesc], List[ChannelDesc]]:
    tasks = [prepare_task(task) for task in tasks]
    channels = []
    return tasks, channels


class GraphExecutorClient:
    def __init__(self, channel: Channel):
        self.stub = GraphExecutorStub(channel)

    async def execute(
        self,
        wflow_id: str,
        wflow_name: str,
        tasks: List[TaskDesc],
        channels: List[ChannelDesc],
        parent_graph_id: Optional[str] = None,
    ) -> GraphExecutionStatus:
        request = GraphExecuteRequest(
            wflow_id,
            wflow_name,
            tasks,
            parent_graph_id,
            channels,
        )

        return await self.stub.Execute(request)

    def status(self, workflow_id: str, graph_id: str) -> GraphExecutionStatus:
        request = GraphStatusRequest(
            workflow_id,
            graph_id,
        )
        return await self.stub.Status(request)

    def stop(self, workflow_id: str, graph_id: str, issue: str) -> GraphExecutionStatus:
        request = GraphStopRequest(
            workflow_id,
            graph_id,
            issue,
        )
        return await self.stub.Stop(request)

    def list(self, workflow_id: str) -> List[GraphExecutionStatus]:
        request = GraphListRequest(workflow_id)
        return (await self.stub.List(request)).graphs
