import inspect
import json
import os.path
import sys
import time
import xmlrpc.client as xmlrpclib
from pathlib import Path
from types import ModuleType
from typing import Any, Dict, Iterable, List, Union, cast, Set

import pkg_resources
from importlib_metadata import packages_distributions  # type: ignore
from stdlib_list import stdlib_list

from lzy.logs.config import get_logger
from lzy.py_env.api import PyEnv, PyEnvProvider

STDLIB_LIST = stdlib_list() if sys.version_info < (3, 10) else sys.stdlib_module_names  # type: ignore

_LOG = get_logger(__name__)


def all_installed_packages() -> Dict[str, str]:
    return {
        entry.project_name: entry.version
        # working_set is actually iterable see sources
        for entry in pkg_resources.working_set  # pylint: disable=not-an-iterable
    }


class AutomaticPyEnvProvider(PyEnvProvider):
    def __init__(self,
                 pypi_cache_file_path: str = "/tmp/pypi_packages_cache",
                 cache_invalidation_period_hours=24,
                 pypi_list_packages_address: str = "https://pypi.python.org/pypi"):
        self.__pypi_list_packages_address = pypi_list_packages_address
        self.__pypi_libs_cache: Set[str]
        self.__pypi_cache_file_path = pypi_cache_file_path

        cache_path = Path(pypi_cache_file_path)
        modification_seconds_diff = time.time() - os.path.getmtime(cache_path)
        modification_hours_diff, _ = divmod(modification_seconds_diff, 3600)
        if cache_path.exists() and modification_hours_diff < cache_invalidation_period_hours:
            _LOG.info("Loading pypi packages cache")
            try:
                with open(cache_path, "r") as file:
                    self.__pypi_libs_cache = set(json.load(file))
            except Exception as e:
                _LOG.warning("Error while pypi packages cache loading", e)
                self.__build_pypi_cache()
        else:
            self.__build_pypi_cache()

    def provide(self, namespace: Dict[str, Any]) -> PyEnv:
        dist_versions: Dict[str, str] = all_installed_packages()

        distributions = packages_distributions()
        remote_packages = {}
        local_modules: List[ModuleType] = []
        parents: List[ModuleType] = []
        seen_modules: Set = set()

        def search(obj: Any) -> None:
            module = inspect.getmodule(obj)
            if module is None:
                return

            if module in seen_modules:
                return
            seen_modules.add(module)

            # try to get module name
            name = module.__name__.split(".")[0]  # type: ignore
            if name in STDLIB_LIST or name in sys.builtin_module_names:
                return

            # and find it among installed ones
            if name in distributions:
                package_name = distributions[name][0]
                if package_name in dist_versions and package_name in self.__pypi_libs_cache:
                    remote_packages[package_name] = dist_versions[package_name]
                    return

            # if module is not found in distributions, try to find it as local one
            if module in local_modules:
                return

            local_modules.append(module)
            for field in dir(module):
                search(getattr(module, field))

            # add all parent modules
            full_module_name = module.__name__
            current_parents: List[ModuleType] = []
            while True:
                last_dot_idx = full_module_name.rfind(".")
                if last_dot_idx == -1:
                    break

                full_module_name = full_module_name[:last_dot_idx]
                # if parent module_name in local_modules already then all parent
                # modules there too already
                parent = sys.modules[full_module_name]
                current_parents.append(parent)
                search(parent)
            parents.extend(reversed(current_parents))

        for _, entry in namespace.items():
            search(entry)

        # remove duplicates and keep order as dict preserves order since python3.7
        all_local_modules = dict.fromkeys(parents)
        all_local_modules.update(dict.fromkeys(reversed(local_modules)))

        def get_path(module: ModuleType) -> Union[List[str], str, None]:
            if not hasattr(module, "__path__") and not hasattr(module, "__file__"):
                return None
            elif not hasattr(module, "__path__"):
                return str(module.__file__)
            else:
                # case for namespace package
                return [module_path for module_path in module.__path__]

        def append_to_module_paths(f: str, paths: List[str]):  # type: ignore
            for module_path in paths:
                if module_path.startswith(f):
                    paths.remove(module_path)
                elif f.startswith(module_path):
                    return
            paths.append(f)

        # reverse to ensure the right order: from leaves to the root
        module_paths: List[str] = []
        for local_module in all_local_modules:
            path = get_path(local_module)
            if path is None:
                _LOG.warning(f"No path for module: {local_module}")
                continue
            elif type(path) == list:
                for p in path:
                    append_to_module_paths(p, module_paths)  # type: ignore
            else:
                append_to_module_paths(path, module_paths)  # type: ignore

        py_version = ".".join(cast(Iterable[str], map(str, sys.version_info[:3])))
        return PyEnv(py_version, remote_packages, module_paths)

    def __build_pypi_cache(self):
        _LOG.info("Building pypi packages cache")
        client = xmlrpclib.ServerProxy(self.__pypi_list_packages_address)
        packages = client.list_packages()
        self.__pypi_libs_cache = set(packages)
        with open(self.__pypi_cache_file_path, "w") as file:
            json.dump(packages, file)
