import inspect
import sys
import pkg_resources

from typing import Any, Dict, Iterable, List, Tuple

import yaml
from importlib_metadata import packages_distributions


# https://stackoverflow.com/a/1883251
def get_base_prefix_compat():
    # Get base/real prefix, or sys.prefix if there is none.
    return getattr(sys, "base_prefix", None) or \
           getattr(sys, "real_prefix", None) or sys.prefix


def in_virtualenv():
    return get_base_prefix_compat() != sys.prefix


def to_str(source: Iterable[Any], delim: str = '.') -> str:
    return delim.join(map(str, source))


exclude = {'lzy-py'}


def all_installed_packages() -> Dict[str, Tuple[str, ...]]:
    return {
        entry.project_name: tuple(entry.version.split('.'))
        for entry in pkg_resources.working_set
        if entry.project_name not in exclude
    }


_installed_versions = {
    "3.7.11": "py37",
    "3.8.12": "py38",
    "3.9.7": "py39",
    # "3.10.0": "py310"
}


def create_yaml(installed_packages: Dict[str, Tuple[str, ...]],
                name: str = 'default') -> Tuple[str, str]:
    # always use only first three numbers, otherwise conda won't find
    python_version = to_str(sys.version_info[:3])
    if python_version in _installed_versions:
        name = _installed_versions[python_version]
        deps: List[Any] = []
    else:
        name = name
        deps = [f'python=={python_version}']

    deps.append('pip')
    deps.append(
        {'pip': [f'{name}=={to_str(version)}'
                 for name, version in
                 installed_packages.items()]}
    )

    conda_yaml = {
        'name': name,
        'dependencies': deps
    }
    return name, yaml.dump(conda_yaml, sort_keys=False)


def select_modules(namespace: Dict[str, Any]) -> \
        Tuple[Dict[str, Tuple[str, ...]], Tuple[str, ...]]:
    dist_versions: Dict[str, Tuple[str, ...]] = all_installed_packages()
    # TODO: this doesn't work for custom modules installed by user, e.g. lzy-py
    # TODO: don't know why

    distributions = packages_distributions()
    packages_with_versions = {}
    packages_without_versions = []
    for k, v in namespace.items():
        # try to get module name
        parent_module = inspect.getmodule(v)
        if parent_module is None:
            continue

        name = inspect.getmodule(v).__name__.split('.')[0] # type: ignore
        if name not in distributions:
            continue

        dist_name = distributions[name][0]
        if dist_name in dist_versions:
            packages_with_versions[dist_name] = dist_versions[dist_name]
        else:
            packages_without_versions.append(dist_name)

    return packages_with_versions, tuple(packages_without_versions)

