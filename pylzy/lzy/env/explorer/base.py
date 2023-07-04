from __future__ import annotations

from abc import abstractmethod
from typing import List, Dict
from typing_extensions import TypeAlias

from lzy.env.base import Deconstructible

ModulePathsList: TypeAlias = List[str]
PackagesDict: TypeAlias = Dict[str, str]


class BaseExplorer(Deconstructible):
    @abstractmethod
    def get_local_module_paths(self) -> ModulePathsList:
        raise NotImplementedError

    @abstractmethod
    def get_installed_pypi_packages(self) -> PackagesDict:
        raise NotImplementedError
