from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import List, Optional

# -------------------------------------------- Graph status
from lzy.proto.bet.priv.v2 import TaskSpec


class Status(ABC):
    pass


class Waiting(Status):
    pass


class TaskStatus:
    # Info about task from scheduler
    pass


class TaskExecutionStatus:
    task_description_id: str
    progress: TaskStatus


class Executing(Status):
    executing_tasks: List[TaskExecutionStatus]


class Completed(Status):
    pass


class Failed(Status):
    pass


# -------------------------------------------- Graph status


@dataclass
class GraphExecutionStatus:
    workflow_id: str
    graph_id: str
    status: Status


class GraphExecutorClient(ABC):
    @abstractmethod
    def execute(
        self,
        workflow_id: str,
        tasks: List[TaskSpec],
        parent_graph_id: Optional[str] = None,
    ) -> GraphExecutionStatus:
        pass

    @abstractmethod
    def status(self, workflow_id: str, graph_id: str) -> GraphExecutionStatus:
        pass

    @abstractmethod
    def stop(self, workflow_id: str, graph_id: str, issue: str) -> GraphExecutionStatus:
        pass

    @abstractmethod
    def list(self, workflow_id: str) -> List[GraphExecutionStatus]:
        pass
