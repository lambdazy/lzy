from dataclasses import dataclass, field

from lzy.env.mixin import WithEnvironmentMixin
from lzy.env.environment import LzyEnvironment
from lzy.env.container.docker import DockerContainer
from lzy.env.container.no_container import NoContainer
from lzy.env.provisioning.provisioning import Provisioning
from lzy.env.python.auto import AutoPythonEnv
from lzy.env.python.manual import ManualPythonEnv


@dataclass
class MyWithEnv(WithEnvironmentMixin):
    env: LzyEnvironment = field(default_factory=LzyEnvironment)


def test_with():
    subject = MyWithEnv()

    container = DockerContainer(image_url='example.com')
    python_env = ManualPythonEnv(
        python_version='3.99',
        local_module_paths=[],
        pypi_packages={},
        pypi_index_url='foo.bar',
    )
    env_vars = {'foo': 'bar'}
    provisioning = Provisioning(cpu_count=1)

    env = LzyEnvironment(
        env_vars=env_vars,
        container=container,
        python_env=python_env,
        provisioning=provisioning,
    )

    assert subject.with_container(container).env.get_container() == container
    assert subject.with_python_env(python_env).env.get_python_env() == python_env
    assert subject.with_provisioning(provisioning).env.get_provisioning() == provisioning
    assert subject.with_env_vars(env_vars).env.get_env_vars() == env_vars

    # check that with doesn't modify original subject:
    assert subject.env.get_env_vars() == {}
    assert subject.env.get_python_env() == AutoPythonEnv()
    assert subject.env.get_container() == NoContainer()
    assert subject.env.get_provisioning() == Provisioning()

    # and after full modification:
    subject = subject.with_env(env)
    assert subject.env.get_container() == container
    assert subject.env.get_python_env() == python_env
    assert subject.env.get_provisioning() == provisioning
    assert subject.env.get_env_vars() == env_vars


def test_decorate():
    subject = MyWithEnv()

    container = DockerContainer(image_url='example.com')
    python_env = ManualPythonEnv(
        python_version='3.99',
        local_module_paths=[],
        pypi_packages={},
        pypi_index_url='foo.bar',
    )
    env_vars = {'foo': 'bar'}
    provisioning = Provisioning(cpu_count=1)

    env = LzyEnvironment(
        env_vars=env_vars,
        container=container,
        python_env=python_env,
        provisioning=provisioning,
    )

    for object_ in (container, python_env, provisioning, env):
        new_subject = object_(subject)
        assert isinstance(new_subject, MyWithEnv)

    for name, object_ in (
        ('container', container),
        ('python_env', python_env),
        ('provisioning', provisioning),
    ):
        new_subject = object_(subject)
        env_field = getattr(new_subject.env, name)
        assert env_field == object_

    new_subject = env(subject)
    assert new_subject.env == env
