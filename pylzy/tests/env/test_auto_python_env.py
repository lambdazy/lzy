import sys
import pytest

from lzy.env.python.auto import AutoPythonEnv
from lzy.env.explorer.auto import AutoExplorer
from lzy.env.explorer.base import BaseExplorer, ModulePathsList, PackagesDict


def test_explorer_factory():
    python_env = AutoPythonEnv()

    assert isinstance(python_env._env_explorer, AutoExplorer)

    class MyExplorer(BaseExplorer):
        def get_local_module_paths(self) -> ModulePathsList:
            return ['foo']

        def get_installed_pypi_packages(self) -> PackagesDict:
            return {'bar': 'baz'}

    def factory(python_env: AutoPythonEnv) -> MyExplorer:
        return MyExplorer()

    python_env = AutoPythonEnv(env_explorer_factory=factory)

    assert isinstance(python_env._env_explorer, MyExplorer)


def test_python_version():
    python_env = AutoPythonEnv()

    etalon = f'{sys.version_info[0]}.{sys.version_info[1]}.{sys.version_info[2]}'

    assert python_env.get_python_version() == etalon


def test_pypi_index_url():
    python_env = AutoPythonEnv()

    # some default, may differ from run environment,
    # more on that in tests/utils/test_pip.py
    assert python_env.get_pypi_index_url().startswith('http')

    python_env = AutoPythonEnv(pypi_index_url='foo')
    assert python_env.get_pypi_index_url() == 'foo'


def test_get_local_module_paths():
    # TBD
    pass


def test_get_pypi_packages():
    # TBD
    pass
