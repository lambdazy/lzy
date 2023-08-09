from __future__ import annotations

import inspect
import functools
import sys
import warnings
from typing import Dict, Any, Set, FrozenSet, List, Tuple, Optional, Iterable, Iterator
from types import ModuleType

from .utils import getmembers


ModulesSet = Set[ModuleType]
ModulesFrozenSet = FrozenSet[ModuleType]
VarsNamespace = Dict[str, Any]


def get_transitive_namespace_dependencies(namespace: VarsNamespace, include_parents: bool = True) -> ModulesFrozenSet:
    """
    Calculates the transitive closure of a namespace in regards to imported modules.

    Although the results of this function are not cached, its performance is
    enerally acceptable due to the caching of intermediate results during execution.
    """

    first_level_dependencies = _get_vars_dependencies(namespace.values())

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

    visited_modules: ModulesSet = set()
    stack = list(initial_dependencies)

    while stack:
        module = stack.pop()
        if module in visited_modules:
            continue

        visited_modules.add(module)

        parent_dependencies = set(_get_parents(module)) if include_parents else set()
        dependencies = get_direct_module_dependencies(module) | parent_dependencies

        stack.extend(
            dep for dep in dependencies
            if dep not in visited_modules
        )

    return frozenset(visited_modules)


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

    members_vars: Iterator[Any] = (var for _, var in members)

    result = _get_vars_dependencies(members_vars)

    # module itself will likely be in _get_vars_dependencies result
    # due to module can contain symbols which defined inside
    return result - {module}


def _get_vars_dependencies(vars_: Iterable[Any]) -> ModulesFrozenSet:
    """
    Return the set of modules that the given vars are defined in.

    This function is not part of the module's main API because it does not
    cache its results, meaning that uncontrolled use can potentially cause
    significant overhead.
    """

    result: ModulesSet = set()
    for var in vars_:
        dependency: Optional[ModuleType] = inspect.getmodule(var)

        if not dependency:
            continue

        result.add(dependency)

    return frozenset(result)


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
