__package__ = None

import os
import sys
from pathlib import Path
from typing import List

import pytest

from lzy.py_env.api import PyEnvProvider
from lzy.py_env.py_env_provider import AutomaticPyEnvProvider


@pytest.fixture(scope="module")
def test_data_dir() -> Path:
    this_test_directory: Path = Path(__file__).parent
    return this_test_directory.parent / 'test_data'


@pytest.fixture(scope="module")
def provider(test_data_dir: Path) -> PyEnvProvider:
    provider: PyEnvProvider = AutomaticPyEnvProvider()

    old_cwd: Path = Path.cwd()
    old_sys_path: List[str] = list(sys.path)

    os.chdir(Path(__file__).parent)
    sys.path.append(str(test_data_dir))

    yield provider

    os.chdir(old_cwd)
    sys.path = old_sys_path


def test_modules_search(provider: PyEnvProvider, test_data_dir: Path):
    # Arrange
    from modules_for_tests.level1.level1 import Level1
    from modules_for_tests_2.level import Level

    level1 = Level1()
    level = Level()

    # Act
    env = provider.provide({"level": level, "level1": level1})
    remote = env.libraries
    local_modules_path = env.local_modules_path

    # Assert
    assert "echo" == level1.echo()

    assert 2 == len(local_modules_path)

    # noinspection DuplicatedCode
    for path in (
        "modules_for_tests",
        "modules_for_tests_2",
    ):
        full_path = test_data_dir / path
        assert str(full_path) in local_modules_path

    for path in (
        "modules_for_tests/level1",
        "modules_for_tests/level1/level2",
        "modules_for_tests/level1/level2/level3",
        "modules_for_tests/level1/level2/level3/level3.py",
        "modules_for_tests/level1/level2/level2.py",
        "modules_for_tests/level1/level1.py",
    ):
        full_path = test_data_dir / path
        assert str(full_path) not in local_modules_path

    assert {"PyYAML", "cloudpickle", "typing_extensions"} == set(remote.keys())


def test_modules_search_2(provider: PyEnvProvider, test_data_dir: Path):
    from modules_for_tests.level1.level2_nb import level_foo

    env = provider.provide({"level_foo": level_foo})
    local_modules_path = env.local_modules_path

    # noinspection DuplicatedCode
    assert 1 == len(local_modules_path)

    for path in (
        "modules_for_tests",
    ):
        full_path = test_data_dir / path
        assert str(full_path) in local_modules_path

    for path in (
        "modules_for_tests/level1",
        "modules_for_tests/level1/level2",
        "modules_for_tests/level1/level2/level3",
        "modules_for_tests/level1/level2/level3/level3.py",
        "modules_for_tests/level1/level2/level2.py",
        "modules_for_tests/level1/level2_nb",
    ):
        full_path = test_data_dir / path
        assert str(full_path) not in local_modules_path


def test_exceptional_builtin(provider: PyEnvProvider):
    import _functools

    env = provider.provide({"_functools": _functools})

    assert len(env.local_modules_path) == 0
    assert len(env.libraries) == 0


def test_exclude_packages(provider: PyEnvProvider, test_data_dir: Path):
    # Arrange
    from modules_for_tests.level1.level1 import Level1
    from modules_for_tests_2.level import Level

    level1 = Level1()
    level = Level()

    # Act
    env = provider.provide({"level": level, "level1": level1}, exclude_packages=("modules_for_tests_2",))
    local_modules_path = env.local_modules_path

    assert 1 == len(local_modules_path)

    for path in (
        "modules_for_tests",
    ):
        full_path = test_data_dir / path
        assert str(full_path) in local_modules_path

    for path in (
        "modules_for_tests/level1",
        "modules_for_tests/level1/level2",
        "modules_for_tests/level1/level2/level3",
        "modules_for_tests/level1/level2/level3/level3.py",
        "modules_for_tests/level1/level2/level2.py",
        "modules_for_tests/level1/level1.py",
        "modules_for_tests2"
    ):
        full_path = test_data_dir / path
        assert str(full_path) not in local_modules_path


def test_namespace_packages(provider: PyEnvProvider, test_data_dir: Path):
    """
    here we are checking that we capturing local google dir but doesn't capture
    google dir from virtualenv's protobuf package
    """

    from google.lzy_namespace import NeverMind

    nevermind = NeverMind()

    env = provider.provide({"nevermind": nevermind})
    remote = env.libraries
    local_modules_path = env.local_modules_path

    assert set(local_modules_path) == {str(test_data_dir / 'google')}
