from abc import ABC, abstractmethod

from lzy.v2.api.runtime.runtime import Runtime
from lzy.v2.serialization.serializer import Serializer


class RuntimeProvider(ABC):
    @abstractmethod
    def get(self, lzy_mount: str, serializer: Serializer) -> Runtime:
        pass