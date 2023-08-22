from __future__ import annotations

from abc import abstractmethod
from typing import TYPE_CHECKING, Dict, Any

from lzy.env.base import Deconstructible
from lzy.env.explorer.base import ModulePathsList, PackagesDict

if TYPE_CHECKING:
    from lzy.env.mixin import WithEnvironmentType

NamespaceType = Dict[str, Any]


class BasePythonEnv(Deconstructible):
    def __call__(self, subject: WithEnvironmentType) -> WithEnvironmentType:
        return subject.with_python_env(self)

    @abstractmethod
    def get_python_version(self) -> str:
        raise NotImplementedError

    @abstractmethod
    def get_local_module_paths(self, namespace: NamespaceType) -> ModulePathsList:
        raise NotImplementedError

    @abstractmethod
    def get_pypi_packages(self, namespace: NamespaceType) -> PackagesDict:
        raise NotImplementedError

    @abstractmethod
    def get_pypi_index_url(self) -> str:
        raise NotImplementedError
