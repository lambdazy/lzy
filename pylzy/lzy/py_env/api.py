from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Dict, Sequence


@dataclass
class PyEnv:
    python_version: str
    libraries: Dict[str, str]  # name -> version
    local_modules_path: Sequence[str]


class PyEnvProvider(ABC):
    @abstractmethod
    def provide(self, namespace: Dict[str, Any]) -> PyEnv:
        pass
