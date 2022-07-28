from typing import TYPE_CHECKING, List, Optional, Tuple
from uuid import uuid4

from grpclib.client import Channel

from ai.lzy.v1.graph.graph_executor_grpc import GraphExecutorStub
from ai.lzy.v1.graph.graph_executor_pb2 import (
    ChannelDesc,
    GraphExecuteRequest,
    GraphExecuteResponse,
    GraphExecutionStatus,
    GraphListRequest,
    GraphListResponse,
    GraphStatusRequest,
    GraphStatusResponse,
    GraphStopRequest,
    GraphStopResponse,
    SlotToChannelAssignment,
    TaskDesc,
)
from lzy.api.v2.proxy_adapter import is_lzy_proxy
from lzy.api.v2.servant.model.converter import to
from lzy.api.v2.servant.model.zygote import python_func_zygote

if TYPE_CHECKING:
    from lzy.api.v2.api.lzy_call import LzyCall


def prepare_task(call: "LzyCall") -> TaskDesc:
    loc_args, non_loc_args = [], []
    for name, arg in call.named_arguments():
        slot_name = f"{call.description}:{name}"
        if is_lzy_proxy(arg):
            non_loc_args.append(slot_name)
        else:
            loc_args.append(slot_name)

    slot_assignments = [
        SlotToChannelAssignment(
            slotName=s_name,
            channelId=str(uuid4()),
        )
        for s_name in non_loc_args
    ]

    wflow = call.parent_wflow
    zygote = python_func_zygote(
        wflow.owner._serializer,
        call.signature.func,
        call.env,
        call.provisioning,
    )

    return TaskDesc(
        id=str(uuid4()),
        zygote=zygote,
        slotAssignments=slot_assignments,
    )


def prepare_tasks_and_channels(
    wflow_id: str,
    tasks: List["LzyCall"],
) -> Tuple[List[TaskDesc], List[ChannelDesc]]:
    _task_descs: List[TaskDesc] = [prepare_task(task) for task in tasks]
    channels: List[ChannelDesc] = []
    return _task_descs, channels


class GraphExecutorClient:
    def __init__(self, channel: Channel):
        self.stub = GraphExecutorStub(channel)

    async def execute(
        self,
        wflow_id: str,
        wflow_name: str,
        tasks: List[TaskDesc],
        channels: List[ChannelDesc],
        parent_graph_id: str = "",
    ) -> GraphExecuteResponse:
        request = GraphExecuteRequest(
            workflowId=wflow_id,
            workflowName=wflow_name,
            tasks=tasks,
            parentGraphId=parent_graph_id,
            channels=channels,
        )

        return await self.stub.Execute(request)

    async def status(
        self,
        workflow_id: str,
        graph_id: str,
    ) -> GraphStatusResponse:
        request = GraphStatusRequest(
            workflowId=workflow_id,
            graphId=graph_id,
        )
        return await self.stub.Status(request)

    async def stop(
        self,
        workflow_id: str,
        graph_id: str,
        issue: str,
    ) -> GraphStopResponse:
        request = GraphStopRequest(
            workflowId=workflow_id,
            graphId=graph_id,
            issue=issue,
        )
        return await self.stub.Stop(request)

    async def list(
        self,
        workflow_id: str,
    ) -> List[GraphExecutionStatus]:
        request = GraphListRequest(workflowId=workflow_id)
        response = await self.stub.List(request)
        return list(response.graphs)
