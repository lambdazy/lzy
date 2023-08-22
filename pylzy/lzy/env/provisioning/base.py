from __future__ import annotations

from abc import abstractmethod
from typing import Sequence, TypeVar, TYPE_CHECKING

from lzy.types import VmSpec
from lzy.env.base import Deconstructible

if TYPE_CHECKING:
    from lzy.env.mixin import WithEnvironmentType


VmSpecType_co = TypeVar('VmSpecType_co', bound=VmSpec, covariant=True)


class BaseProvisioning(Deconstructible):
    @abstractmethod
    def __call__(self, subject: WithEnvironmentType) -> WithEnvironmentType:
        raise NotImplementedError

    @abstractmethod
    def resolve_pool(self, vm_specs: Sequence[VmSpecType_co]) -> VmSpecType_co:
        raise NotImplementedError

    @abstractmethod
    def validate(self) -> None:
        raise NotImplementedError
