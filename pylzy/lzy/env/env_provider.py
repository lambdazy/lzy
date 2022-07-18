from abc import ABC, abstractmethod
from typing import Any, Dict, Optional

from lzy.env.env import EnvSpec


class EnvProvider(ABC):
    @abstractmethod
    def provide(self, namespace: Optional[Dict[str, Any]] = None) -> EnvSpec:
        pass
