import sys
import pytest
import importlib_metadata

from packaging.tags import PythonVersion

import envzy.classify
from envzy import AutoExplorer, ModulePathsList, PackagesDict, BaseExplorer, BadPypiIndex

from lzy.env.python.auto import AutoPythonEnv


def test_explorer_factory() -> None:
    python_env = AutoPythonEnv()

    assert isinstance(python_env._env_explorer, AutoExplorer)

    class MyExplorer(BaseExplorer):
        def get_local_module_paths(self, namespace) -> ModulePathsList:
            return ['foo']

        def get_pypi_packages(self, namespace) -> PackagesDict:
            return {'bar': 'baz'}

    def factory(python_env: AutoPythonEnv) -> MyExplorer:
        return MyExplorer()

    python_env = AutoPythonEnv(env_explorer_factory=factory)

    assert isinstance(python_env._env_explorer, MyExplorer)
    assert python_env.get_pypi_packages({}) == {'bar': 'baz'}
    assert python_env.get_local_module_paths({}) == ['foo']


def test_python_version() -> None:
    python_env = AutoPythonEnv()

    etalon = f'{sys.version_info[0]}.{sys.version_info[1]}.{sys.version_info[2]}'

    assert python_env.get_python_version() == etalon


def test_pypi_index_url() -> None:
    python_env = AutoPythonEnv()

    # some default, may differ from run environment,
    # more on that in tests/utils/test_pip.py
    assert python_env.get_pypi_index_url().startswith('http')

    python_env = AutoPythonEnv(pypi_index_url='foo')
    assert python_env.get_pypi_index_url() == 'foo'


def test_get_modules_and_paths(
    with_test_modules,
    get_test_data_path,
    monkeypatch,
) -> None:
    # more of this test at explorer/test_classify.py, here
    # i'm testing just interface

    def mock_find_distribution_at_pypi(self, name: str, version: str):
        return 'foo'

    def mock_check_distribution_platform_at_pypi(
        self, pypi_index_url: str, name: str, version: str, target_python: PythonVersion
    ):
        return True

    monkeypatch.setattr(
        envzy.classify.ModuleClassifier,
        '_find_distribution_at_pypi',
        mock_find_distribution_at_pypi,
    )

    monkeypatch.setattr(
        envzy.classify.ModuleClassifier,
        '_check_distribution_platform_at_pypi',
        mock_check_distribution_platform_at_pypi,
    )

    import empty_module

    python_env = AutoPythonEnv()
    namespace = {'foo': empty_module}

    assert python_env.get_local_module_paths(namespace) == \
        [str(get_test_data_path('empty_module.py'))]

    assert python_env.get_pypi_packages(namespace) == {}

    import typing_extensions

    namespace = {'foo': typing_extensions}

    assert python_env.get_local_module_paths(namespace) == []
    assert python_env.get_pypi_packages(namespace) == {
        'typing-extensions': importlib_metadata.distribution('typing_extensions').version
    }


@pytest.mark.vcr
def test_validate_pypi_index_url(pypi_index_url_testing, monkeypatch):
    import envzy.pypi
    monkeypatch.setattr(envzy.pypi, 'VALIDATE_PYPI_INDEX_URL', True)

    python_env = AutoPythonEnv()
    python_env.validate()

    python_env = python_env.with_fields(pypi_index_url=pypi_index_url_testing)
    python_env.validate()

    python_env = python_env.with_fields(pypi_index_url='https://example.com')
    with pytest.raises(BadPypiIndex):
        python_env.validate()
