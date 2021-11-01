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

def get_python_env_as_yaml(name='default'):
    # get only first three numbers, otherwise conda won't find
    python_version = sys.version_info[:3]
    conda_yaml = {
        'name': name,
        'dependencies': [
            f'python=={to_str(python_version)}',
            'pip',
            {'pip': [f'{name}=={to_str(version)}'
                     for name, version in
                     get_installed_packages().items()]}
        ]
    }

    return yaml.dump(conda_yaml, sort_keys=False)
