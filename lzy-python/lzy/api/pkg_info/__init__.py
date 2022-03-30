import inspect
import sys
from types import ModuleType
from typing import Any, Dict, Iterable, List, Tuple, Union

import pkg_resources
import requests
import yaml
from importlib_metadata import packages_distributions
from stdlib_list import stdlib_list


# https://stackoverflow.com/a/1883251
def get_base_prefix_compat():
    # Get base/real prefix, or sys.prefix if there is none.
    return getattr(sys, "base_prefix", None) or \
           getattr(sys, "real_prefix", None) or \
           sys.prefix


def in_virtualenv():
    return get_base_prefix_compat() != sys.prefix


def to_str(source: Iterable[Any], delim: str = ".") -> str:
    return delim.join(map(str, source))


def all_installed_packages() -> Dict[str, Tuple[str, ...]]:
    return {
        entry.project_name: tuple(entry.version.split("."))
        # working_set is actually iterable see sources
        for entry in pkg_resources.working_set  # pylint: disable=not-an-iterable
    }


_installed_versions = {
    "3.7.11": "py37",
    "3.8.12": "py38",
    "3.9.7": "py39",
    # "3.10.0": "py310"
}

pypi_existence_cache: Dict[str, bool] = dict()


def create_yaml(installed_packages: Dict[str, Tuple[str, ...]], name: str = "default") -> Tuple[str, str]:
    # always use only first three numbers, otherwise conda won't find
    python_version = to_str(sys.version_info[:3])
    if python_version in _installed_versions:
        name = _installed_versions[python_version]
        deps: List[Any] = []
    else:
        deps = [f"python=={python_version}"]

    deps.append("pip")
    deps.append(
        {
            "pip": [
                f"{name}=={to_str(version)}"
                for name, version in installed_packages.items()
            ]
        }
    )

    conda_yaml = {"name": name, "dependencies": deps}
    return name, yaml.dump(conda_yaml, sort_keys=False)


def exists_in_pypi(package_name: str) -> bool:
    if package_name in pypi_existence_cache:
        return pypi_existence_cache[package_name]

    response = requests.get("https://pypi.python.org/pypi/{}/json"
                            .format(package_name))
    result: bool = 200 <= response.status_code < 300
    pypi_existence_cache[package_name] = result
    return result


version = "3.9" if sys.version_info > (3, 9) else None
STDLIB_LIST = stdlib_list(version)


def select_modules(namespace: Dict[str, Any]) -> Tuple[Dict[str, Tuple[str, ...]], List[str]]:
    dist_versions: Dict[str, Tuple[str, ...]] = all_installed_packages()

    distributions = packages_distributions()
    remote_packages = {}
    local_modules: List[ModuleType] = []
    parents: List[ModuleType] = []

    def search(obj: Any) -> None:
        module = inspect.getmodule(obj)
        if module is None:
            return

        # try to get module name
        name = module.__name__.split(".")[0]  # type: ignore
        if name in STDLIB_LIST:
            return

        # and find it among installed ones
        if name in distributions:
            package_name = distributions[name][0]
            if package_name in dist_versions and exists_in_pypi(package_name):
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
            last_dot_idx = full_module_name.rfind('.')
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

    def get_path(module: ModuleType) -> Union[List[str], str]:
        if not hasattr(module, '__path__'):
            return str(module.__file__)
        else:
            # case for namespace package
            return [module_path for module_path in module.__path__]

    def append_to_module_paths(p: str, module_paths: List[str]):  # type: ignore
        for module_path in module_paths:
            if module_path.startswith(p):
                module_paths.remove(module_path)
            elif p.startswith(module_path):
                return
        module_paths.append(p)

    # reverse to ensure the right order: from leaves to the root
    module_paths: List[str] = []
    for local_module in all_local_modules:
        path = get_path(local_module)
        if type(path) == list:
            for p in path:
                append_to_module_paths(p, module_paths)  # type: ignore
        else:
            append_to_module_paths(path, module_paths)  # type: ignore
    return remote_packages, module_paths
