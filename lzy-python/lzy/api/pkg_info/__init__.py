import inspect
import sys
from types import ModuleType
from typing import Any, Dict, Iterable, List, Tuple

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


def select_modules(namespace: Dict[str, Any]) -> Tuple[Dict[str, Tuple[str, ...]], List[ModuleType]]:
    dist_versions: Dict[str, Tuple[str, ...]] = all_installed_packages()

    distributions = packages_distributions()
    remote_packages = {}
    local_modules: List[ModuleType] = []

    def search(obj: Any) -> None:
        # try to get module name
        parent_module = inspect.getmodule(obj)
        if parent_module is None:
            return

        module = inspect.getmodule(obj)
        if not module:
            return

        name = module.__name__.split(".")[0]  # type: ignore
        if name in stdlib_list():
            return

        if name in distributions and distributions[name][0] in dist_versions and exists_in_pypi(distributions[name][0]):
            remote_packages[distributions[name][0]] = dist_versions[distributions[name][0]]
        else:
            if module in local_modules:
                return
            local_modules.append(module)
            for field in dir(module):
                search(getattr(module, field))

    for _, entry in namespace.items():
        search(entry)

    local_modules = list(
        dict.fromkeys(local_modules))  # remove duplicates and keep order as dict preserves order since python3.7
    local_modules.reverse()
    return remote_packages, local_modules  # reverse to ensure the right order: from leaves to the root
