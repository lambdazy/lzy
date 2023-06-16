import pytest

import lzy.exceptions
from lzy.api.v1.env import Env


@pytest.fixture
def env():
    return Env(python_version='3.7.11')


@pytest.mark.vcr
def test_validate_pypi_index_url(env, pypi_index_url_testing):
    assert not env.pypi_index_url
    env.validate()

    env.pypi_index_url = pypi_index_url_testing
    env.validate()

    env.pypi_index_url = 'https://example.com'
    with pytest.raises(lzy.exceptions.BadPypiIndex):
        env.validate()


def test_generate_pypi_index_config(env, pypi_index_url_testing):
    def get_pip_deps(config):
        dependencies = config.get('dependencies', [])
        pip_deps = [item for item in dependencies if isinstance(item, dict) and 'pip' in item]

        assert len(pip_deps) <= 1

        return pip_deps

    config = env.generate_conda_config()
    pip_deps = get_pip_deps(config)
    assert not pip_deps

    env.pypi_index_url = pypi_index_url_testing
    config = env.generate_conda_config()

    pip_deps = get_pip_deps(config)
    assert len(pip_deps) == 1
    assert pip_deps[0]['pip'] == [f'--index-url {pypi_index_url_testing}']

    assert pypi_index_url_testing in env.get_conda_yaml()
