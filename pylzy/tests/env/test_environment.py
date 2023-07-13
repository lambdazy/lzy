import pytest

from lzy.env.environment import LzyEnvironment
from lzy.env.container.docker import DockerContainer
from lzy.env.container.no_container import NoContainer
from lzy.env.provisioning.provisioning import Provisioning
from lzy.env.python.auto import AutoPythonEnv
from lzy.env.python.manual import ManualPythonEnv


def test_defaults():
    env = LzyEnvironment()

    assert env.get_env_vars() == {}
    assert env.get_python_env() == AutoPythonEnv()
    assert env.get_container() == NoContainer()
    assert env.get_provisioning() == Provisioning()


def test_defaults_override():
    env = LzyEnvironment()

    container = DockerContainer(image_url='example.com')
    python_env = ManualPythonEnv(
        python_version='3.99',
        local_module_paths=[],
        pypi_packages={},
        pypi_index_url='foo.bar',
    )
    env_vars = {'foo': 'bar'}
    provisioning = Provisioning(cpu_count=1)

    assert env.with_fields(env_vars=env_vars).get_env_vars() == env_vars
    assert env.with_fields(container=container).get_container() == container
    assert env.with_fields(python_env=python_env).get_python_env() == python_env
    assert env.with_fields(provisioning=provisioning).get_provisioning() == provisioning

    env = env.with_fields(
        env_vars=env_vars,
        container=container,
        python_env=python_env,
        provisioning=provisioning,
    )

    assert env.get_env_vars() == env_vars
    assert env.get_python_env() == python_env
    assert env.get_container() == container
    assert env.get_provisioning() == provisioning


def test_combine_provisionin():
    container1 = DockerContainer(image_url='example1.com')
    container2 = DockerContainer(image_url='example2.com')

    python_env1 = ManualPythonEnv(
        python_version='3.99',
        local_module_paths=['a', 'b'],
        pypi_packages={'foo': 'bar'},
        pypi_index_url='foo.bar',
    )
    python_env2 = ManualPythonEnv(
        python_version='3.999',
        local_module_paths=[],
        pypi_packages={},
        pypi_index_url='foo.bar.baz',
    )
    env_vars1 = {'foo1': 'bar1'}
    env_vars2 = {'foo2': 'bar2'}

    provisioning1 = Provisioning(cpu_count=1, ram_size_gb=2)
    provisioning2 = Provisioning(gpu_count=3, cpu_count=4)

    env = LzyEnvironment(
        env_vars=env_vars1,
        container=container1,
        python_env=python_env1,
        provisioning=provisioning1
    )

    # no merging:
    assert env.with_fields(env_vars=env_vars2).env_vars == env_vars2
    assert env.with_fields(container=container2).container == container2
    assert env.with_fields(python_env=python_env2).python_env == python_env2

    # here is a catch:
    assert env.with_fields(provisioning=provisioning2).provisioning != provisioning1
    assert env.with_fields(provisioning=provisioning2).provisioning != provisioning2
    assert env.with_fields(provisioning=provisioning2).provisioning == Provisioning(
        cpu_count=4,
        ram_size_gb=2,
        gpu_count=3
    )
