from __future__ import annotations

import inspect
import functools
from typing import Dict, Any, Set, FrozenSet, List, Tuple, Optional, Iterable, Iterator
from types import ModuleType


ModulesSet = Set[ModuleType]
ModulesFrozenSet = FrozenSet[ModuleType]


def get_transitive_namespace_dependencies(namespace: Dict[str, Any]) -> ModulesFrozenSet:
    """
    Calculates the transitive closure of a namespace in regards to imported modules.

    Although the results of this function are not cached, its performance is
    enerally acceptable due to the caching of intermediate results during execution.
    """

    first_level_dependencies = _get_vars_dependencies(namespace.values())

    result: ModulesSet = set()

    for module in first_level_dependencies:
        module_dependencies = get_transitive_module_dependencies(module)
        result.update(module_dependencies)

    return frozenset(result)


@functools.lru_cache(maxsize=None)
def get_transitive_module_dependencies(module: ModuleType) -> ModulesFrozenSet:
    """
    Retrieve all transient dependencies of a module.

    This function caches the results to optimize performance on
    repeated calls with the same arguments.
    """

    initial_dependencies = get_direct_module_dependencies(module)

    visited_modules: ModulesSet = set()
    stack = list(initial_dependencies)

    while stack:
        module = stack.pop()
        if module in visited_modules:
            continue

        visited_modules.add(module)
        dependencies = get_direct_module_dependencies(module)

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

    members: List[Tuple[str, Any]] = inspect.getmembers(module)
    members_vars: Iterator[Any] = (var for _, var in members)
    return _get_vars_dependencies(members_vars)


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
        if dependency:
            result.add(dependency)

    return frozenset(result)
