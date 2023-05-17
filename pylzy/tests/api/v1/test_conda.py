import logging
import pytest

from lzy.api.v1.env import Env


def test_conda_generate_in_cache():
    env = Env(
        python_version="3.7.11",
        libraries={"pylzy": "0.0.0"}
    )
    yaml = env.get_conda_yaml()

    assert "name: py37" in yaml
    assert "pylzy==0.0.0" in yaml


def test_conda_generate_not_in_cache(caplog):
    version = "3.7.9999"

    env = Env(
        python_version=version,
        libraries={"pylzy": "0.0.0"}
    )

    with caplog.at_level(logging.WARNING, logger='lzy.api.v1.env'):
        yaml = env.get_conda_yaml()

    assert any(
        (
            f"Installed python version ({version})" in record.message and
            record.levelno == logging.WARNING
        )
        for record in caplog.records
    )

    assert "name: default" in yaml
    assert "pylzy==0.0.0" in yaml
