from __future__ import annotations

from dataclasses import dataclass

from .base import BasePythonEnv, ModulePathsList, PackagesDict


@dataclass
class ManualPythonEnv(BasePythonEnv):
    python_version: str
    local_module_paths: ModulePathsList
    pypi_packages: PackagesDict
    pypi_index_url: str

    def get_python_version(self) -> str:
        return self.python_version

    def get_local_module_paths(self) -> ModulePathsList:
        return self.local_module_paths

    def get_pypi_packages(self) -> PackagesDict:
        return self.pypi_packages

    def get_pypi_index_url(self) -> str:
        return self.pypi_index_url
