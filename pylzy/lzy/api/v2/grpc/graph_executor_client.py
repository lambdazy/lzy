from typing import List, Optional

from grpclib.client import Channel

from ai.lzy.v1.graph import (
    GraphExecutionStatus,
    GraphExecuteRequest,
    GraphListRequest,
    TaskDesc,
    GraphStatusRequest,
    GraphStopRequest,
)
from ai.lzy.v1.graph.graph_executor_grpc import GraphExecutorStub


class GraphExecutorClient:
    def __init__(self, channel: Channel):
        self.stub = GraphExecutorStub(channel)

    async def execute(
        self,
        workflow_id: str,
        tasks: List[TaskDesc],
        parent_graph_id: Optional[str] = None,
    ) -> GraphExecutionStatus:
        request = GraphExecuteRequest(
            workflow_id,
            workflow_id,
            tasks,
            parent_graph_id,
            None,
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
