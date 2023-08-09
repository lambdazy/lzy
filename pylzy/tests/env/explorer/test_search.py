import builtins
import inspect
import typing
import sys
import yaml

from types import ModuleType
from typing import cast

import pytest
import typing_extensions

from lzy.env.explorer.search import (
    get_direct_module_dependencies,
    get_transitive_module_dependencies,
    get_transitive_namespace_dependencies,
    _get_vars_dependencies,
    ModulesSet
)


@pytest.fixture
def loaders() -> ModulesSet:
    """
    Fixture returns two standard modules, which are "dependency" of any other module
    from __loader__ and __spec__ variables.
    I'm not sure that this code will be working same way with every python version :(
    """
    importlib_bootstrap = inspect.getmodule(pytest.__spec__)
    importlib_bootstrap_external = inspect.getmodule(pytest.__loader__)

    assert importlib_bootstrap
    assert importlib_bootstrap_external

    return {importlib_bootstrap, importlib_bootstrap_external}


def test_get_vars_dependencies(with_test_modules: None) -> None:
    assert _get_vars_dependencies([str]) == {builtins}

    import modules_for_tests.level1.level1 as level1
    import modules_for_tests.level1.level2.level2 as level2
    import modules_for_tests.level1.level2.level3.level3 as level3
    import modules_for_tests.level1.level2_nb as level2_nb

    for mod in (level1, level2, level3, level2_nb):
        assert _get_vars_dependencies([mod]) == {mod}

    assert _get_vars_dependencies([level1.Level1]) == {level1}
    assert _get_vars_dependencies([level1.Level2]) == {level2}
    assert _get_vars_dependencies([level2_nb.Level2]) == {level2}
    assert _get_vars_dependencies([level2.Level2]) == {level2}
    assert _get_vars_dependencies([level2.Level3]) == {level3}
    assert _get_vars_dependencies([level2.cast]) == {typing}
    assert _get_vars_dependencies([level3.yaml]) == {yaml}


def test_get_direct_module_dependencies(with_test_modules, loaders: ModulesSet) -> None:
    import modules_for_tests.level1.level1 as level1
    import modules_for_tests.level1.level2.level2 as level2
    import modules_for_tests.level1.level2.level3.level3 as level3
    import modules_for_tests.level1.level2_nb as level2_nb

    def assert_dependencies(module, etalon):
        assert get_direct_module_dependencies(module) - loaders == etalon

    assert_dependencies(
        level1,
        {level2, typing_extensions if sys.version_info < (3, 10) else typing}
    )
    assert_dependencies(level2,  {level3, typing})
    assert_dependencies(level2_nb, {level2})
    assert_dependencies(level3, {yaml})

    import modules_for_tests_3.empty as empty
    import modules_for_tests_3.simple_class as simple_class
    import modules_for_tests_3.one_dependency as one_dependency
    import modules_for_tests_3.two_dependencies as two_dependencies
    import modules_for_tests_3.second_import as second_import

    assert_dependencies(empty, set())
    assert_dependencies(simple_class, set())
    assert_dependencies(one_dependency, {simple_class})
    assert_dependencies(two_dependencies, {one_dependency})
    assert_dependencies(second_import, {two_dependencies, empty})


def test_get_transitive_module_dependencies(with_test_modules) -> None:
    import modules_for_tests_3 as top
    import modules_for_tests_3.empty as empty
    import modules_for_tests_3.one_dependency as one_dependency
    import modules_for_tests_3.simple_class as simple_class
    import modules_for_tests_3.two_dependencies as two_dependencies
    import modules_for_tests_3.second_import as second_import

    all_modules = {
        empty,
        one_dependency,
        simple_class,
        two_dependencies,
        second_import,
        top
    }

    # on python 3.7 it have len == 388 XD
    empty_deps = get_transitive_module_dependencies(empty, include_parents=False)

    def assert_dependencies(module, etalon):
        assert get_transitive_module_dependencies(module, include_parents=False) - empty_deps == etalon

    assert_dependencies(empty, set())
    assert_dependencies(simple_class, set())
    assert_dependencies(one_dependency, {simple_class})
    assert_dependencies(two_dependencies, {simple_class, one_dependency})
    assert_dependencies(second_import, {simple_class, one_dependency, two_dependencies, empty})

    import empty_module as empty_module

    empty_deps = get_transitive_module_dependencies(empty_module, include_parents=True)
    for module in all_modules:
        assert get_transitive_module_dependencies(module, include_parents=True) - empty_deps == all_modules


def test_get_transitive_namespace_dependencies(with_test_modules) -> None:
    import empty_module as empty_module
    import modules_for_tests_3.empty as empty
    import modules_for_tests_3.one_dependency as one_dependency
    import modules_for_tests_3.simple_class as simple_class
    import modules_for_tests_3.two_dependencies as two_dependencies
    import modules_for_tests_3.second_import as second_import

    empty_deps = get_transitive_module_dependencies(empty_module, include_parents=False)

    def assert_dependencies(namespace, etalon):
        assert get_transitive_namespace_dependencies(namespace, include_parents=False) - empty_deps == etalon

    assert_dependencies({'foo': empty}, {empty})
    assert_dependencies({'foo': empty, 'bar': simple_class.SimpleClass}, {empty, simple_class})
    assert_dependencies({'foo': second_import.empty}, {empty})
    assert_dependencies({'foo': second_import.two_dependencies}, {one_dependency, two_dependencies, simple_class})