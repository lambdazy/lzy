from __future__ import annotations

import sys
from dataclasses import dataclass, field
from typing import Callable, List, Optional, Dict, Any
from typing_extensions import Self

from lzy.env.explorer.base import BaseExplorer
from lzy.env.explorer.auto import AutoExplorer
from .base import BasePythonEnv, ModulePathsList, PackagesDict, NamespaceType


def _auto_explorer_factory(auto_py_env: AutoPythonEnv) -> AutoExplorer:
    return AutoExplorer(
        pypi_index_url=auto_py_env.get_pypi_index_url(),
        additional_pypi_packages=auto_py_env.additional_pypi_packages
    )


@dataclass(frozen=True)
class AutoPythonEnv(BasePythonEnv):
    pypi_index_url: Optional[str] = None
    additional_pypi_packages: PackagesDict = field(default_factory=dict)
    env_explorer_factory: Callable[[Self], BaseExplorer] = _auto_explorer_factory

    _env_explorer: Optional[BaseExplorer] = None

    def __post_init__(self):
        # because we are frozen
        if not self._env_explorer:
            object.__setattr__(self, '_env_explorer', self.env_explorer_factory(self))

    @property
    def env_explorer(self) -> BaseExplorer:
        assert self._env_explorer
        return self._env_explorer

    def get_python_version(self) -> str:
        version_info: List[str] = [str(i) for i in sys.version_info[:3]]
        python_version = '.'.join(version_info)
        return python_version

    def get_local_module_paths(self, namespace: NamespaceType) -> ModulePathsList:
        return self.env_explorer.get_local_module_paths(namespace)

    def get_pypi_packages(self, namespace: NamespaceType) -> PackagesDict:
        return self.env_explorer.get_pypi_packages(namespace)
