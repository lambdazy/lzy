from abc import abstractmethod, ABC
from dataclasses import dataclass
from typing import Optional, Iterable

from lzy.api.v1.servant.model.channel import Bindings


@dataclass
class ExecutionResult:
    stdout: str
    stderr: str
    returncode: int


@dataclass
class ExecutionValue:
    name: str
    entry_id: Optional[str]


@dataclass
class InputExecutionValue(ExecutionValue):
    hash: Optional[str]


@dataclass
class ExecutionDescription:
    name: str
    snapshot_id: str
    inputs: Iterable[InputExecutionValue]
    outputs: Iterable[ExecutionValue]


class Execution(ABC):
    # pylint: disable=invalid-name
    @abstractmethod
    def id(self) -> str:
        pass

    @abstractmethod
    def bindings(self) -> Bindings:
        pass

    @abstractmethod
    def wait_for(self) -> ExecutionResult:
        pass

    def __enter__(self):
        return self

    def __exit__(self, *_):
        return False