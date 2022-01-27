import inspect
from stdlib_list import stdlib_list
import sys
from types import ModuleType
from typing import Any, Dict, Iterable, List, Tuple, Set

import pkg_resources
import requests
import yaml
from importlib_metadata import packages_distributions


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


def select_modules(namespace: Dict[str, Any]) -> Tuple[Dict[str, Tuple[str, ...]], Set[ModuleType]]:
    dist_versions: Dict[str, Tuple[str, ...]] = all_installed_packages()

    distributions = packages_distributions()
    remote_packages = {}
    local_modules: Set[ModuleType] = set()
    for name, entry in namespace.items():
        # try to get module name
        parent_module = inspect.getmodule(entry)
        if parent_module is None:
            continue

        module = inspect.getmodule(entry)
        if not module:
            continue

        name = module.__name__.split(".")[0]  # type: ignore
        if name in stdlib_list():
            continue

        if name not in distributions:
            local_modules.add(module)
            continue

        dist_name = distributions[name][0]
        if dist_name in dist_versions and exists_in_pypi(dist_name):
            remote_packages[dist_name] = dist_versions[dist_name]
        else:
            local_modules.add(module)

    return remote_packages, local_modules
