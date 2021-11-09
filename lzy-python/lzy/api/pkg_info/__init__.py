import sys
from typing import Any, Iterable

import pkg_resources
import yaml


# https://stackoverflow.com/a/1883251
def get_base_prefix_compat():
    # Get base/real prefix, or sys.prefix if there is none.
    return getattr(sys, "base_prefix", None) or \
           getattr(sys, "real_prefix", None) or sys.prefix


def in_virtualenv():
    return get_base_prefix_compat() != sys.prefix


def to_str(source: Iterable[Any], delim: str = '.') -> str:
    return delim.join(map(str, source))


def get_installed_packages():
    exclude = {'lzy-py'}
    return {
        entry.project_name: entry.version.split('.')
        for entry in pkg_resources.working_set
        if entry.project_name not in exclude
    }


_installed_versions = {
    "3.7.11": "py37",
    "3.8.12": "py38",
    "3.9.7": "py39",
    # "3.10.0": "py310"
}


def get_python_env_as_yaml(name='default'):
    # always use only first three numbers, otherwise conda won't find
    python_version = to_str(sys.version_info[:3])
    if python_version in _installed_versions:
        name = _installed_versions[python_version]
        deps = []
    else:
        name = 'default'
        deps = [f'python=={python_version}']

    deps.append('pip')
    deps.append(
        {'pip': [f'{name}=={to_str(version)}'
                 for name, version in
                 get_installed_packages().items()]}
    )

    conda_yaml = {
        'name': name,
        'dependencies': deps
    }
    return name, yaml.dump(conda_yaml, sort_keys=False)
