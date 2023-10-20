from __future__ import annotations

import inspect
import functools
import sys
import warnings
from typing import Dict, Any, Set, FrozenSet, List, Tuple, Optional, Iterable, Iterator
from types import ModuleType

from lzy.logs.config import get_logger
from .utils import getmembers, get_stdlib_module_names, get_builtin_module_names, is_lazy_module

ModulesSet = Set[ModuleType]
ModulesFrozenSet = FrozenSet[ModuleType]
VarsNamespace = Dict[str, Any]

logger = get_logger(__name__)


def get_transitive_namespace_dependencies(namespace: VarsNamespace, include_parents: bool = True) -> ModulesFrozenSet:
    """
    Calculates the transitive closure of a namespace in regards to imported modules.

    Although the results of this function are not cached, its performance is
    enerally acceptable due to the caching of intermediate results during execution.
    """

    first_level_dependencies = _get_vars_dependencies(namespace.items())

    result: ModulesSet = set(first_level_dependencies)

    for module in first_level_dependencies:
        module_dependencies = get_transitive_module_dependencies(module, include_parents=include_parents)
        result.update(module_dependencies)

    return frozenset(result)


@functools.lru_cache(maxsize=None)
def get_transitive_module_dependencies(module: ModuleType, include_parents: bool = True) -> ModulesFrozenSet:
    """
    Retrieve all transient dependencies of a module.

    This function caches the results to optimize performance on
    repeated calls with the same arguments.

    """

    # NB: we are adding include_parents argument mostly for tests, because
    # with include_parents True it is too difficult to check how search algorithm
    # searching dependencies, because with include_parents=True it will follow
    # all modules of any package.

    # NB: we a considering "parents" of module as a dependencies of this module:
    # if we a using module `foo.bar`, in that case when we have imported foo.bar,
    # some code inside foo/__init__.py might have been executed, so it could affect
    # behaviour of `foo.bar`.

    parent_dependencies = set(_get_parents(module)) if include_parents else set()
    initial_dependencies = get_direct_module_dependencies(module) | parent_dependencies

    logger.debug(
        'initial dependencies for module %s: %s; starting DFS',
        module.__name__, [m.__name__ for m in initial_dependencies]
    )

    stack = list(initial_dependencies)
    seen_modules: ModulesSet = set(initial_dependencies)

    while stack:
        submodule = stack.pop()

        parent_dependencies = set(_get_parents(submodule)) if include_parents else set()
        dependencies = get_direct_module_dependencies(submodule) | parent_dependencies
        new_dependencies = [dep for dep in dependencies if dep not in seen_modules]

        if not new_dependencies:
            continue

        logger.debug(
            'new dependencies for module %s from module %s: %s',
            module.__name__, submodule.__name__, [m.__name__ for m in new_dependencies]
        )

        stack.extend(new_dependencies)
        seen_modules.update(new_dependencies)

    logger.debug(
        'final dependencies for module %s: %s',
        module.__name__, [m.__name__ for m in seen_modules]
    )

    return frozenset(seen_modules)


@functools.lru_cache(maxsize=None)
def get_direct_module_dependencies(module: ModuleType) -> ModulesFrozenSet:
    """
    Return the direct dependencies of a module.

    This function caches its results for performance optimization.
    """

    # All real import-time warnings user already saw when he did
    # imports at his code. Actually we are supressing only
    # "useless" import warnings triggered by our DFS, not by user actions.
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")

        # NB: we are using our getmembers instead of inspect.getmembers,
        # because original one are raising TypeError in case of
        # getmembers(torch.ops), and we are catching it in our clone
        members: List[Tuple[str, Any]] = getmembers(module)

    result = _get_vars_dependencies(members)

    # module itself will likely be in _get_vars_dependencies result
    # due to module can contain symbols which defined inside
    return result - {module}


def _get_vars_dependencies(vars_: Iterable[Tuple[str, Any]]) -> ModulesFrozenSet:
    """
    Return the set of modules that the given vars are defined in.

    This function is not part of the module's main API because it does not
    cache its results, meaning that uncontrolled use can potentially cause
    significant overhead.
    """

    result: Dict[ModuleType, Any] = {}
    for var_name, var in vars_:
        dependency: Optional[ModuleType] = inspect.getmodule(var)

        if not dependency or _filter_dependency(dependency):
            continue

        result[dependency] = var_name

    return frozenset(result)


@functools.lru_cache(maxsize=None)
def _get_search_stoplist() -> FrozenSet[str]:
    builtins = get_builtin_module_names()
    stdlib = get_stdlib_module_names()
    additional = {
        'pkg_resources'
    }

    return frozenset(builtins | stdlib | additional)


@functools.lru_cache(maxsize=None)
def _filter_dependency(module: ModuleType) -> bool:
    if is_lazy_module(module):
        return True

    stoplist = _get_search_stoplist()

    parts = module.__name__.split('.')
    while parts:
        name = '.'.join(parts)
        if name in stoplist:
            return True

        parts.pop()

    return False


def _get_parents(module: ModuleType) -> Iterator[ModuleType]:
    """
    Return the set of modules which hierarchically parents of given module
    from python packages perspective.

    """

    name = module.__name__
    parts = name.split('.')
    parts.pop()

    while parts:
        parent_name = '.'.join(parts)

        # it may be absent in strange cases, if someone playing with
        # __name__, for example, symbol `torch.ops.pyops`
        # have a __name__ = 'torch.ops.torch.ops' but there is
        # no such key in sys.modules
        parent = sys.modules.get(parent_name)

        if parent:
            yield parent

        parts.pop()
