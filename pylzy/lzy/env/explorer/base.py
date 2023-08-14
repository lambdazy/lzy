from __future__ import annotations

from abc import abstractmethod
from typing import List, Dict
from typing_extensions import TypeAlias

from lzy.env.base import Deconstructible, WithLogger
from .search import VarsNamespace

ModulePathsList: TypeAlias = List[str]
PackagesDict: TypeAlias = Dict[str, str]


class BaseExplorer(Deconstructible, WithLogger):
    @abstractmethod
    def get_local_module_paths(self, namespace: VarsNamespace) -> ModulePathsList:
        raise NotImplementedError

    @abstractmethod
    def get_pypi_packages(self, namespace: VarsNamespace) -> PackagesDict:
        raise NotImplementedError
