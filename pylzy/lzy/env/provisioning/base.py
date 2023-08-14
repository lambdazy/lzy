from __future__ import annotations

from abc import abstractmethod
from typing import Sequence, TYPE_CHECKING

from lzy.types import VmSpec
from lzy.env.base import Deconstructible

if TYPE_CHECKING:
    from lzy.env.mixin import WithEnvironmentType


class BaseProvisioning(Deconstructible):
    @abstractmethod
    def __call__(self, subject: WithEnvironmentType) -> WithEnvironmentType:
        raise NotImplementedError

    @abstractmethod
    def resolve_pool(self, vm_specs: Sequence[VmSpec]) -> VmSpec:
        raise NotImplementedError

    @abstractmethod
    def validate(self) -> None:
        raise NotImplementedError
