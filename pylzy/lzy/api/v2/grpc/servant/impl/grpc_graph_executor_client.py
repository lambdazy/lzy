from typing import List, Optional

from lzy.api.v2.grpc.servant.api.graph_executor_client import (
    GraphExecutionStatus,
    GraphExecutorClient,
    TaskSpec,
)


class GrpcGraphExecutorClient(GraphExecutorClient):
    def execute(
        self,
        workflow_id: str,
        tasks: List[TaskSpec],
        parent_graph_id: Optional[str] = None,
    ) -> GraphExecutionStatus:
        # TODO: implement
        pass

    def status(self, workflow_id: str, graph_id: str) -> GraphExecutionStatus:
        # TODO: implement
        pass

    def stop(self, workflow_id: str, graph_id: str, issue: str) -> GraphExecutionStatus:
        # TODO: implement
        pass

    def list(self, workflow_id: str) -> List[GraphExecutionStatus]:
        # TODO: implement
        pass
