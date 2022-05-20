from abc import ABC, abstractmethod
from typing import Dict, Any, Optional

from lzy.env.env import Env


class EnvProvider(ABC):
    @abstractmethod
    def for_op(self, namespace: Optional[Dict[str, Any]] = None) -> Env:
        pass
