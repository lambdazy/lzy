from __future__ import annotations

import sys
import types
from functools import lru_cache
from inspect import isclass, getmro
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import List, Any, Tuple, Dict, FrozenSet

import importlib_metadata
from importlib_metadata import Distribution

from lzy.utils.paths import change_working_directory


def getmembers(object: Any, predicate=None) -> List[Tuple[str, Any]]:
    """Return all members of an object as (name, value) pairs sorted by name.
    Optionally, only return members that satisfy a given predicate.

    NB: this function is a copypaste from inspect.py with small addition:
    we are catching TypeError in addition to AttributeError
    and ModuleNotFoundError in additition to other AttributeError at other tre/except.
    """
    if isclass(object):
        mro = (object,) + getmro(object)
    else:
        mro = ()
    results: List[Tuple[str, Any]] = []
    processed = set()
    names = dir(object)
    # :dd any DynamicClassAttributes to the list of names if object is a class;
    # this may result in duplicate entries if, for example, a virtual
    # attribute with the same name as a DynamicClassAttribute exists
    try:
        for base in object.__bases__:
            for k, v in base.__dict__.items():
                if isinstance(v, types.DynamicClassAttribute):
                    names.append(k)
    except (AttributeError, TypeError):
        pass
    for key in names:
        # First try to get the value via getattr.  Some descriptors don't
        # like calling their __get__ (see bug #1785), so fall back to
        # looking in the __dict__.
        try:
            value = getattr(object, key)
            # handle the duplicate key
            if key in processed:
                raise AttributeError
        except (AttributeError, ModuleNotFoundError):
            for base in mro:
                if key in base.__dict__:
                    value = base.__dict__[key]
                    break
            else:
                # could be a (currently) missing slot member, or a buggy
                # __dir__; discard and move on
                continue
        if not predicate or predicate(value):
            results.append((key, value))
        processed.add(key)
    results.sort(key=lambda pair: pair[0])
    return results


def check_url_is_local_file(url: str) -> bool:
    file_scheme = 'file://'

    if not url.startswith(file_scheme):
        return False

    raw_path = url[len(file_scheme):]
    path = Path(raw_path)

    # idk if there may be relative paths in direct_url.json,
    # but if it is, next code will work badly with it.
    if not path.is_absolute():
        return False

    return path.exists()


@lru_cache(maxsize=None)
def get_stdlib_module_names() -> FrozenSet[str]:
    try:
        return frozenset(sys.stdlib_module_names)  # type: ignore
    except AttributeError:
        # python_version < 3.10
        from stdlib_list import stdlib_list

        return frozenset(stdlib_list())


@lru_cache(maxsize=None)
def get_builtin_module_names() -> FrozenSet[str]:
    return frozenset(sys.builtin_module_names)


@lru_cache(maxsize=None)  # cache size is about few MB
def get_files_to_distributions() -> Dict[str, Distribution]:
    result = {}

    # NB: importlib_metadata.Distribution calls may return different
    # results in depends from cwd.
    # So we are moving cwd into tmp dir in name of results repeatability.
    # In case of PermissionError, location of tmp dir can be moved with
    # TMPDIR env variable.
    with TemporaryDirectory() as tmp:
        with change_working_directory(tmp):
            for distribution in importlib_metadata.distributions():
                for filename in distribution.files:
                    fullpath = distribution.locate_file(filename)
                    result[str(fullpath)] = distribution

    return result
