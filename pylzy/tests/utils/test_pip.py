import os
import sys
from pathlib import Path
from contextlib import contextmanager

import pytest
from packaging.version import Version
from pypi_simple import PYPI_SIMPLE_ENDPOINT

from lzy.utils.pip import Pip

DEFAULT_URL = PYPI_SIMPLE_ENDPOINT
TEST_URL = 'https://test.pypi.org/simple'


@contextmanager
def create_pip():
    old_state = Pip._shared_state

    Pip._shared_state = {}
    pip = Pip()

    yield pip

    Pip._shared_state = old_state


def test_version(monkeypatch):
    with create_pip() as pip:
        assert isinstance(pip.version, Version)
        assert pip.version.major >= 10

    bad_version = '9.0.0'

    def bad_version_call(*args, **kwargs):
        return f'pip {bad_version} from <directory> (python <python version>)'

    with monkeypatch.context() as m:
        m.setattr(Pip, 'call', bad_version_call)

        with pytest.raises(
            RuntimeError,
            match=(
                rf'pip minimum required version is {Pip.min_version} '
                rf'and you have {bad_version}, please upgrade it'
            )
        ):
            with create_pip() as pip:
                pass


def test_config(monkeypatch, get_test_data_path):
    # default config
    # but non-env values could leaks from system configs outside test environment
    with create_pip() as pip:
        for key, value in {
            ':env:.disable-pip-version-check': 'true',
            ':env:.index-url': DEFAULT_URL,
            ':env:.yes': 'true',
        }.items():
            assert pip.config[key] == value

    # here we turn of loading of any config files
    with monkeypatch.context() as m:
        m.setenv('PIP_CONFIG_FILE', os.devnull)
        with create_pip() as pip:
            assert pip.config == {
                ':env:.disable-pip-version-check': 'true',
                ':env:.index-url': DEFAULT_URL,
                ':env:.yes': 'true',
                ':env:.config-file': os.devnull,
            }

    # settings via env works normally
    with monkeypatch.context() as m:
        m.setenv('PIP_INDEX_URL', 'foobar')
        with create_pip() as pip:
            assert pip.config[':env:.index-url'] == 'foobar'

    # you can't change pip_yes because we explicitly set an true at Pip.call
    with monkeypatch.context() as m:
        m.setenv('PIP_YES', 'false')
        with create_pip() as pip:
            assert pip.config[':env:.yes'] == 'true'

    # values from PIP_CONFIG_FILE should be the most significant and ignore configs
    # from the system
    with monkeypatch.context() as m:
        m.setenv('PIP_CONFIG_FILE', str(get_test_data_path('pip.conf')))
        with create_pip() as pip:
            assert pip.config[':env:.index-url'] == DEFAULT_URL
            assert pip.config[':env:.config-file'] == str(get_test_data_path('pip.conf'))
            assert pip.config['global.index-url'] == TEST_URL


def test_pip_index_url(monkeypatch, get_test_data_path):
    # this default set at tox.ini
    with create_pip() as pip:
        assert pip.index_url == DEFAULT_URL

    with monkeypatch.context() as m:
        m.setenv('PIP_INDEX_URL', 'foobar')
        with create_pip() as pip:
            assert pip.index_url == 'foobar'

    # no env, config at PIP_CONFIG_FILE
    with monkeypatch.context() as m:
        m.delenv('PIP_INDEX_URL')
        m.setenv('PIP_CONFIG_FILE', str(get_test_data_path('pip.conf')))
        with create_pip() as pip:
            assert pip.index_url == TEST_URL

    # no env, config at site
    with monkeypatch.context() as m:
        m.delenv('PIP_INDEX_URL')

        config = get_test_data_path('pip.conf').read_text(encoding='utf-8')
        site_config_path = Path(sys.prefix) / 'pip.conf'

        try:
            site_config_path.write_text(config, encoding='utf-8')

            with create_pip() as pip:
                assert pip.index_url == TEST_URL
        finally:
            site_config_path.unlink()

    # no configs no env
    with monkeypatch.context() as m:
        m.delenv('PIP_INDEX_URL')
        m.setenv('PIP_CONFIG_FILE', os.devnull)

        with create_pip() as pip:
            assert pip.index_url is None
