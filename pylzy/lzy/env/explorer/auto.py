from __future__ import annotations

from dataclasses import dataclass
from typing import List

from .base import BaseExplorer, ModulePathsList, PackagesDict


@dataclass
class AutoExplorer(BaseExplorer):
    pypi_index_url: str
    exclude_packages: List[str]

    def get_local_module_paths(self) -> ModulePathsList:
        raise NotImplementedError

    def get_installed_pypi_packages(self) -> PackagesDict:
        raise NotImplementedError
