from __future__ import annotations

from abc import abstractmethod
from typing import TYPE_CHECKING, Dict, Any, Optional

from lzy.env.base import Deconstructible
from lzy.env.explorer.base import ModulePathsList, PackagesDict
from lzy.utils.pip import Pip
from lzy.utils.pypi import PYPI_INDEX_URL_DEFAULT, validate_pypi_index_url


if TYPE_CHECKING:
    from lzy.env.mixin import WithEnvironmentType

NamespaceType = Dict[str, Any]


class BasePythonEnv(Deconstructible):
    pypi_index_url: Optional[str]

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

    def get_pypi_index_url(self) -> str:
        return self.pypi_index_url or Pip().index_url or PYPI_INDEX_URL_DEFAULT

    def validate(self) -> None:
        pypi_index_url = self.get_pypi_index_url()
        validate_pypi_index_url(pypi_index_url)
