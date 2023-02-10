import inspect
import json
import os
import time
from collections import defaultdict

import sys
from pathlib import Path
from types import ModuleType
from typing import Any, Dict, Iterable, List, Union, cast, Set

import pkg_resources
import requests
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
                 existed_cache_file_path: str = "/tmp/pypi_existed_packages_cache",
                 nonexistent_cache_file_path: str = "/tmp/pypi_nonexistent_packages_cache",
                 cache_invalidation_period_hours: int = 24):
        self.__existed_cache: Dict[str, List[str]] = defaultdict(list)
        self.__nonexistent_cache: Dict[str, List[str]] = defaultdict(list)

        self.__existed_cache_file_path = existed_cache_file_path
        self.__nonexistent_cache_file_path = nonexistent_cache_file_path
        self.__nonexistent_cache_creation_time = time.time()

        existed_cache_path = Path(existed_cache_file_path)
        if existed_cache_path.exists():
            try:
                with open(existed_cache_path, "r") as file:
                    self.__existed_cache.update(json.load(file))
            except Exception as e:
                _LOG.warning("Error while pypi existed packages cache loading", e)

        nonexistent_cache_path = Path(nonexistent_cache_file_path)
        if nonexistent_cache_path.exists():
            try:
                with open(nonexistent_cache_path, "r") as file:
                    creation_time = float(file.readline())
                    modification_seconds_diff = time.time() - creation_time
                    modification_hours_diff, _ = divmod(modification_seconds_diff, 3600)
                    if modification_hours_diff >= cache_invalidation_period_hours:
                        return  # do not load cache
                    self.__nonexistent_cache_creation_time = creation_time
                    self.__nonexistent_cache.update(json.load(file))
            except Exception as e:
                _LOG.warning("Error while pypi nonexistent packages cache loading", e)

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
            all_from_pypi: bool = False
            if name in distributions:
                all_from_pypi = len(distributions[name]) > 0
                for package_name in distributions[name]:
                    if package_name in dist_versions and self.__exists_in_pypi(package_name,
                                                                               dist_versions[package_name]):
                        remote_packages[package_name] = dist_versions[package_name]
                    else:
                        all_from_pypi = False

            if all_from_pypi:
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
                return [module_path for module_path in set(module.__path__)]

        def append_to_module_paths(f: str, paths: List[str]):  # type: ignore
            for module_path in paths:
                if module_path.startswith(f"{f}{os.sep}"):
                    paths.remove(module_path)
                elif f.startswith(f"{module_path}{os.sep}"):
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

        self.__save_pypi_cache()
        py_version = ".".join(cast(Iterable[str], map(str, sys.version_info[:3])))
        return PyEnv(py_version, remote_packages, module_paths)

    def __save_pypi_cache(self):
        with open(self.__existed_cache_file_path, "w") as file:
            json.dump(self.__existed_cache, file)
        with open(self.__nonexistent_cache_file_path, "w") as file:
            file.write(f"{str(self.__nonexistent_cache_creation_time)}\n")
            json.dump(self.__nonexistent_cache, file)

    def __exists_in_pypi(self, package_name: str, package_version: str) -> bool:
        if package_name in self.__existed_cache and package_version in self.__existed_cache[package_name]:
            return True
        elif package_name in self.__nonexistent_cache and package_version in self.__nonexistent_cache[package_name]:
            return False

        _LOG.info(f"Checking {package_name}=={package_version} exists in pypi...")
        with requests.Session() as session:
            session.max_redirects = (
                5  # limit redirects to handle possible pypi incidents with redirect cycles
            )
            response = session.get(f"https://pypi.python.org/pypi/{package_name}/{package_version}/json")
        result: bool = 200 <= response.status_code < 300
        if result:
            self.__existed_cache[package_name].append(package_version)
        else:
            self.__nonexistent_cache[package_name].append(package_version)
        return result
