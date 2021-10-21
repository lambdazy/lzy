
# https://stackoverflow.com/a/1883251
import sys
from typing import Any

import pkg_resources

from model.env import PyEnv


def get_base_prefix_compat():
    # Get base/real prefix, or sys.prefix if there is none.
    return getattr(sys, "base_prefix", None) or \
           getattr(sys, "real_prefix", None) or sys.prefix


def in_virtualenv():
    return get_base_prefix_compat() != sys.prefix


def to_str(tup: tuple[Any]) -> tuple[str]:
    return tuple(map(str, tup))


def get_installed_packages():
    return {
        entry.project_name: entry.version.split('.')
        for entry in pkg_resources.working_set
    }


def save_python_env(name='default'):
    if not in_virtualenv():
        # TODO: better exception
        raise ValueError('Script started not from virtualenv')

    return PyEnv(name, interpreter_version=to_str(sys.version_info),
                 packages=get_installed_packages())
