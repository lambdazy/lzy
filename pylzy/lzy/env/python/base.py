from __future__ import annotations

from abc import abstractmethod
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Dict, Any

from lzy.env.base import Deconstructible
from lzy.env.explorer.base import ModulePathsList, PackagesDict

if TYPE_CHECKING:
    from lzy.env.mixin import WithEnvironmentType


@dataclass(frozen=True)
class BasePythonEnv(Deconstructible):
    _namespace: Dict[str, Any] = field(default_factory=dict, init=False)

    def __call__(self, subject: WithEnvironmentType) -> WithEnvironmentType:
        return subject.with_python_env(self)

    @abstractmethod
    def get_python_version(self) -> str:
        raise NotImplementedError

    @abstractmethod
    def get_local_module_paths(self) -> ModulePathsList:
        raise NotImplementedError

    @abstractmethod
    def get_pypi_packages(self) -> PackagesDict:
        raise NotImplementedError

    @abstractmethod
    def get_pypi_index_url(self) -> str:
        raise NotImplementedError
