import inspect
import json
import os
import sys
import time

from collections import defaultdict
from hashlib import md5
from pathlib import Path
from types import ModuleType
from typing import Any, Dict, Iterable, List, cast, Set, FrozenSet, Optional

import requests
import importlib_metadata

from pypi_simple import PYPI_SIMPLE_ENDPOINT, PyPISimple

from lzy.logs.config import get_logger
from lzy.py_env.api import PyEnv, PyEnvProvider
from lzy.utils.paths import get_cache_path
from lzy.utils.pypi import check_package_version_exists


try:
    STDLIB_LIST: FrozenSet[str] = sys.stdlib_module_names  # type: ignore
except AttributeError:
    # python_version < 3.10
    from stdlib_list import stdlib_list

    STDLIB_LIST: FrozenSet[str] = frozenset(stdlib_list())  # type: ignore

CACHE_TYPE = Dict[str, Set[str]]

INDEX_FILE = 'pypi_index_url'
EXISTING_FILE = 'pypi_existing_packages'
NONEXISTING_FILE = 'pypi_nonexisting_packages'

_LOG = get_logger(__name__)


class AutomaticPyEnvProvider(PyEnvProvider):
    def __init__(
        self,
        *,
        drop_cache: bool = False,
        nonexisting_cache_invalidation_time: int = 24 * 60 * 60,
        pypi_index_url: str = PYPI_SIMPLE_ENDPOINT,
    ):
        self._nonexisting_cache_invalidation_time: int = nonexisting_cache_invalidation_time
        self._pypi_index_url: str = pypi_index_url
        pypi_index_url_md5 = md5(pypi_index_url.encode()).hexdigest()

        self._existing_cache: CACHE_TYPE = defaultdict(set)
        self._nonexisting_cache: CACHE_TYPE = defaultdict(set)

        self._existing_cache_file_path = get_cache_path(pypi_index_url_md5, EXISTING_FILE)
        self._nonexisting_cache_file_path = get_cache_path(pypi_index_url_md5, NONEXISTING_FILE)
        self._index_file_path = get_cache_path(pypi_index_url_md5, INDEX_FILE)

        if not drop_cache:
            self._load_pypi_cache()

    @property
    def pypi_index_url(self):
        return self._pypi_index_url

    def provide(self, namespace: Dict[str, Any], exclude_packages: Iterable[str] = tuple()) -> PyEnv:
        exclude = set(exclude_packages)

        distributions = importlib_metadata.packages_distributions()

        remote_packages = {}
        local_packages: Set[str] = set()

        remote_packages_files: Set[str] = set()
        local_modules: List[ModuleType] = []
        seen_modules: Set[ModuleType] = set()
        namespace_prefixes: Set[str] = set()

        def search(obj: Any) -> None:
            module: Optional[ModuleType] = inspect.getmodule(obj)
            if (
                module is None or
                module in seen_modules
            ):
                return

            seen_modules.add(module)

            top_level: str = module.__name__.split(".")[0]
            filename = getattr(module, '__file__', None)

            if (
                top_level in STDLIB_LIST or
                top_level in sys.builtin_module_names or
                top_level in exclude or
                # maybe in previous iterations we findout that this
                # module is from remote package; also it means that we
                # already processed this package.
                # NB: namespace packages doesn't have a filename and
                # we cannot be sure we already processed em.
                filename and filename in remote_packages_files
            ):
                return

            # try to find module as installed package
            all_from_pypi: bool = False
            if top_level in distributions:
                # NB: one module can belong to several packages
                packages = distributions[top_level]
                all_from_pypi = bool(packages)

                for package_name in packages:

                    # XXX: At the moment I'm not fully realizing in which cases one package
                    # could get into this cycle several times.
                    # Probably while parents bypass for local packages.
                    # But I place this stub here until next refactoring
                    # to prevent self._exists_in_pypi calling several times for the
                    # same package.
                    if package_name in remote_packages:
                        continue
                    if package_name in local_packages:
                        all_from_pypi = False
                        continue

                    distribution = importlib_metadata.distribution(package_name)

                    # NB: here we checking if package installed as editable installation
                    # look at https://github.com/python/importlib_metadata/issues/404 discussion
                    is_editable = bool(distribution.read_text('direct_url.json'))

                    package_version = distribution.version

                    if (
                        not is_editable and
                        package_version and
                        self._exists_in_pypi(package_name, package_version)
                    ):
                        # TODO: If top_level is a namespace package, we can add here package which
                        # a really unused
                        remote_packages[package_name] = package_version

                        files = [str(distribution.locate_file(f)) for f in distribution.files]
                        remote_packages_files.update(files)
                    else:
                        local_packages.add(package_name)
                        all_from_pypi = False

            # Namespace package doesn't have a __file__;
            # Later any module with __file__.startswith(namespace_prefix)
            # will be treated as __path__ = [namespace_prefix] in goal of
            # our local_modules_paths detection;
            # Also this way we are extracting local and only local part of this
            # namespace
            if not all_from_pypi and not filename:
                prefixes = getattr(module, '__path__', [])
                namespace_prefixes.update(prefixes)
                return

            if (
                # all packages, containing this top_level are installed from pypi
                all_from_pypi or
                # we just findout that this module is from remote package
                filename in remote_packages_files
            ):
                return
            # else:
            #     # this module we considers as local module we wanna to locate
            #     True == not all_from_pypi and and filename and filename not remote_packages

            local_modules.append(module)

            for field_name in dir(module):
                symbol = getattr(module, field_name)

                # recursion, baby!
                search(symbol)

            for parent in _get_module_parents(module):
                # more recursion!
                search(parent)

        for entry in namespace.values():
            search(entry)

        all_module_paths: List[str] = []
        for local_module in local_modules:
            path = _get_path(local_module, namespace_prefixes)

            if not path:
                _LOG.warning("no path for module: %s", local_module)
                continue

            all_module_paths.extend(path)

        module_paths = _get_most_common_paths(all_module_paths)

        self._save_pypi_cache()

        version_info: List[str] = [str(i) for i in sys.version_info[:3]]
        python_version = '.'.join(version_info)

        return PyEnv(
            python_version=python_version,
            libraries=remote_packages,
            local_modules_path=module_paths,
            pypi_index_url=self._pypi_index_url,
        )

    def _load_cache_file(self, path: Path, cache: CACHE_TYPE) -> None:
        if not path.exists():
            return

        try:
            with path.open('r', encoding='utf-8') as file_:
                cache_data: Dict[str, List[str]] = json.load(file_)
                for package, versions in cache_data.items():
                    cache[package].update(versions)
        except Exception:
            _LOG.warning("Error while loading cache file %s", path, exc_info=True)

    def _load_pypi_cache(self) -> None:
        self._load_cache_file(
            self._existing_cache_file_path,
            self._existing_cache
        )

        if not self._nonexisting_cache_file_path.exists():
            return

        modification_time = self._nonexisting_cache_file_path.stat().st_mtime
        modification_seconds_diff = time.time() - modification_time

        if modification_seconds_diff > self._nonexisting_cache_invalidation_time:
            return

        self._load_cache_file(
            self._nonexisting_cache_file_path,
            self._nonexisting_cache
        )

    def _save_cache_file(self, path: Path, cache: CACHE_TYPE) -> None:
        def serialize_sets(obj):
            if isinstance(obj, set):
                return list(obj)

            return obj

        with path.open('w', encoding='utf-8') as file_:
            json.dump(cache, file_, default=serialize_sets)

    def _save_pypi_cache(self):
        self._save_cache_file(
            self._existing_cache_file_path,
            self._existing_cache
        )
        self._save_cache_file(
            self._nonexisting_cache_file_path,
            self._nonexisting_cache
        )

        # just to know which cache folder which cache contains
        self._index_file_path.write_text(self._pypi_index_url, encoding='utf-8')

    def _exists_in_pypi(self, name: str, version: str) -> bool:
        req_str = f"{name}=={version}"
        pypi_str = f"pypi index {self.pypi_index_url}"

        if version in self._existing_cache[name]:
            _LOG.debug("%s exists in local cache of existing packages of %s", req_str, pypi_str)
            return True
        if version in self._nonexisting_cache[name]:
            _LOG.debug("%s exists in local cache of nonexisting packages of %s", req_str, pypi_str)
            return False

        result = check_package_version_exists(
            pypi_index_url=self.pypi_index_url,
            name=name,
            version=version
        )

        _LOG.debug("%s%s exists at %s", req_str, "" if result else " doesn't", pypi_str)

        if result:
            self._existing_cache[name].add(version)
        else:
            self._nonexisting_cache[name].add(version)
        return result


def _path_startswith(path_to_test: str, path_substr: str) -> bool:
    """
    >>> _path_startswith('/a/b/c', '/a/b')
    True
    >>> _path_startswith('/a/b/c', '/a/b/')
    True
    >>> _path_startswith('/a/b/c', '/a/c')
    False

    """

    if not path_substr.endswith(os.sep):
        path_substr += os.sep

    return path_to_test.startswith(path_substr)


def _get_path(module: ModuleType, namespace_prefixes: Set[str]) -> List[str]:
    filename = getattr(module, '__file__', None)

    # this way we can findout that this module is a part of early noticed namespace packages
    # and we wanna use __path__ of namespace instead of this modules paths because
    # in other case we just lose namespace's __path__
    if filename:
        suitable_namespace_prefixes = [
            p for p in namespace_prefixes if _path_startswith(filename, p)
        ]
        if suitable_namespace_prefixes:
            return suitable_namespace_prefixes

    if getattr(module, '__path__', None):
        # In case of namespace packages which have parts across the disk.
        # For example, google.__path__ ==
        # _NamespacePath(['<site>/lib/python3.7/site-packages/google', '<home>/repos/lzy/pylzy/google']),
        # But google.rpc.status_pb2 doesn't have an __path__ attr, only __file__
        return list(module.__path__)

    # modules outside a packages doesn't have __path__
    if filename:
        return [cast(str, module.__file__)]

    # for example, interactive python shell will have an module __main__
    # but will not have __path__ and __file__
    return []


def _get_most_common_paths(paths: List[str]) -> List[str]:
    """
    >>> _get_most_common_paths(['a/b/c', 'a/b/c/d', 'a/b/d', 'a/b', 'b/c/d', 'b/c', 'a/b'])
    ['a/b', 'b/c']
    """

    paths_to_test = set(paths[:])
    new_paths = []

    while paths_to_test:
        path_to_test = paths_to_test.pop()

        most_common = True
        for path_to_compare in list(paths_to_test):
            if path_to_test.startswith(f'{path_to_compare}{os.sep}'):
                most_common = False
                break

            if path_to_compare.startswith(f'{path_to_test}{os.sep}'):
                paths_to_test.remove(path_to_compare)

        if most_common:
            new_paths.append(path_to_test)

    return sorted(new_paths)


def _get_module_parents(module: ModuleType) -> Iterable[ModuleType]:
    module_name = module.__name__
    parts = module_name.split('.')

    # If we at module a.b.c, then after first parts.pop(),
    # parts will be ['a', 'b'].
    # And where is my `do { } while` (╯ ° □ °) ╯ (┻━┻)
    parts.pop()

    while parts:
        parent_module_name = '.'.join(parts)
        parent = sys.modules[parent_module_name]

        yield parent

        parts.pop()
