from __future__ import annotations

from platform import python_version

import pytest

from lzy.api.v1 import Lzy, op
from lzy.env.base import NotSpecified
from lzy.env.container.no_container import NoContainer
from lzy.env.container.docker import DockerContainer, DockerPullPolicy
from lzy.env.provisioning.provisioning import Provisioning, NO_GPU, Any
from lzy.env.provisioning.score import minimum_score_function, maximum_score_function
from lzy.env.python.auto import AutoPythonEnv
from lzy.env.python.manual import ManualPythonEnv
from lzy.core.call import LzyCall
from lzy.core.workflow import LzyWorkflow
from lzy.exceptions import BadProvisioning
from lzy.types import VmSpec
from tests.api.v1.mocks import RuntimeMock, StorageRegistryMock


@op
def func() -> None:
    pass


@pytest.fixture
def lzy(vm_specs):
    return Lzy(
        runtime=RuntimeMock(
            vm_specs=vm_specs,
        ),
        storage_registry=StorageRegistryMock(),
    ).with_manual_python_env(
        python_version='3.7.11',
        local_module_paths=["local_module_paths"],
        pypi_packages={"pylzy": "0.0.0"}
    )


def assert_pool_spec(wf: LzyWorkflow, etalon: VmSpec) -> None:
    vm_pool_spec = wf.first_call.vm_spec  # type: ignore

    assert vm_pool_spec == etalon


def test_description(lzy):
    description = "my favourite func"

    @op(description=description)
    def func_description() -> None:
        pass

    with lzy.workflow("test") as wf:
        func_description()

    assert description == wf.first_call.description


def test_invalid_workflow_provisioning(lzy):
    provisioning = Provisioning(
        gpu_type=NO_GPU,
        gpu_count=4
    )
    with pytest.raises(BadProvisioning, match=r"gpu_type is set to NO_GPU while gpu_count"):
        with lzy.workflow("test").with_provisioning(provisioning):
            func()


def test_unavailable_provisioning(lzy):
    # NB: single gpu_count is raising other error
    for field, value in (
        ('cpu_count', 1000),
        ('ram_size_gb', 1002),
        ('cpu_type', 'foo'),
        ('gpu_type', 'bar'),
    ):
        kwargs = {field: value}

        with pytest.raises(BadProvisioning, match=r"not a single one available spec from"):
            with lzy.workflow("test").with_provisioning(**kwargs):
                func()


def test_default_provisioning(lzy, vm_spec_small):
    with lzy.workflow("test") as wf:
        func()

    provisioning: Provisioning = wf.first_call.get_provisioning()

    assert provisioning.cpu_type is NotSpecified
    assert provisioning.gpu_type is NotSpecified
    assert provisioning.cpu_count is NotSpecified
    assert provisioning.ram_size_gb is NotSpecified
    assert provisioning.gpu_count is NotSpecified

    vm_spec: VmSpec = provisioning._as_vm_spec()
    assert vm_spec.cpu_type == ''
    assert vm_spec.gpu_type == NO_GPU
    assert vm_spec.cpu_count == 0
    assert vm_spec.ram_size_gb == 0
    assert vm_spec.gpu_count == 0

    assert_pool_spec(wf, vm_spec_small)


def test_workflow_provisioning(lzy, vm_spec_large):
    with lzy.workflow("test").with_provisioning(gpu_count=1, gpu_type='V100') as wf:
        func()

    provisioning = wf.first_call.get_provisioning()

    assert provisioning.cpu_type is NotSpecified
    assert provisioning.cpu_count is NotSpecified
    assert provisioning.ram_size_gb is NotSpecified
    assert provisioning.gpu_count == 1
    assert provisioning.gpu_type == 'V100'

    assert_pool_spec(wf, vm_spec_large)


def test_op_provisioning(lzy, vm_spec_large):
    @Provisioning(gpu_count=1, cpu_count=8)
    @op
    def func_with_provisioning() -> None:
        pass

    with lzy.workflow("test") \
            .with_provisioning(
                Provisioning(gpu_type='V100', cpu_count=32, gpu_count=4)
            ) as wf:
        func_with_provisioning()

    provisioning = wf.first_call.get_provisioning()

    assert provisioning.cpu_type is NotSpecified
    assert provisioning.cpu_count == 8
    assert provisioning.ram_size_gb is NotSpecified
    assert provisioning.gpu_count == 1
    assert provisioning.gpu_type == 'V100'

    assert_pool_spec(wf, vm_spec_large)


def test_op_provisioning_invalid(lzy):
    @Provisioning(gpu_count=8)
    @op
    def func_with_provisioning() -> None:
        pass

    with pytest.raises(BadProvisioning, match="gpu_type is set to NO_GPU while gpu_count"):
        with lzy.workflow("test").with_provisioning(gpu_type=NO_GPU):
            func_with_provisioning()


def test_provisioning_score_func(lzy, vm_spec_large, vm_spec_small):
    with lzy.workflow("test") as wf:
        func()

    assert_pool_spec(wf, vm_spec_small)

    with lzy.workflow("test").with_provisioning(
        Provisioning(score_function=minimum_score_function)
    ) as wf:
        func()

    assert_pool_spec(wf, vm_spec_small)

    with lzy.workflow("test").with_provisioning(
        Provisioning(score_function=maximum_score_function, gpu_type=Any)
    ) as wf:
        func()

    assert_pool_spec(wf, vm_spec_large)


def test_default_python_env():
    # NB: lzy fixture have python_env overrided
    lzy = Lzy(runtime=RuntimeMock(), storage_registry=StorageRegistryMock())
    with lzy.workflow("test") as wf:
        func()

    call: LzyCall = wf.first_call
    python_env = call.final_env.get_python_env()
    assert isinstance(python_env, AutoPythonEnv)
    assert python_version() == python_env.get_python_version()

    with pytest.raises(RuntimeError, match="Network is disabled"):
        call.get_pypi_packages()

    with pytest.raises(RuntimeError, match="Network is disabled"):
        call.get_local_module_paths()


def test_workflow_env(lzy):
    @op
    def func_without_env() -> None:
        pass

    with lzy.workflow("test").with_python_env(
        ManualPythonEnv(
            python_version="3.8.6",
            pypi_packages={"pylzy": "1.1.1"},
            local_module_paths=['/a/b/c']
        )
    ) as wf:
        func_without_env()

    call: LzyCall = wf.first_call
    assert call.get_python_version() == "3.8.6"
    assert "pylzy" in call.get_pypi_packages()
    assert "/a/b/c" in call.get_local_module_paths()
    assert isinstance(call.final_env.get_container(), NoContainer)


def test_op_env(lzy):
    @ManualPythonEnv(python_version="3.9.15", pypi_packages={"cloudpickle": "1.1.1"}, local_module_paths=['lol_kek'])
    @op
    def func_with_env() -> None:
        pass

    with lzy.workflow("test").with_manual_python_env(
        python_version="3.8.6",
        pypi_packages={"pylzy": "1.1.1"},
        local_module_paths=[],
    ) as wf:
        func_with_env()

    call: LzyCall = wf.first_call
    assert "3.9.15" == call.get_python_version()
    assert "pylzy" not in call.get_pypi_packages()
    assert "cloudpickle" in call.get_pypi_packages()
    assert "lol_kek" in call.get_local_module_paths()


def test_docker_wf(lzy):
    with lzy.workflow("test").with_docker_container(registry='foo', image='bar') as wf:
        func()

    call: LzyCall = wf.first_call
    assert call.get_python_version() is not None
    assert isinstance(call.get_container(), DockerContainer)
    assert call.get_container().get_registry() == 'foo'
    assert call.get_container().get_image() == 'bar'
    assert call.get_container().get_pull_policy() == DockerPullPolicy.IF_NOT_EXISTS
    assert call.get_container().get_password() is None
    assert call.get_container().get_username() is None


def test_docker_op(lzy):
    @DockerContainer(registry='foo1', image='bar1', pull_policy=DockerPullPolicy.ALWAYS)
    @op
    def func_with_docker() -> None:
        pass

    with lzy.workflow("test").with_docker_container(
        registry='foo', image='bar', username='baz'
    ) as wf:
        func_with_docker()

    call: LzyCall = wf.first_call
    assert call.get_python_version() is not None
    assert isinstance(call.get_container(), DockerContainer)
    assert call.get_container().get_registry() == 'foo1'
    assert call.get_container().get_image() == 'bar1'
    assert call.get_container().get_pull_policy() == DockerPullPolicy.ALWAYS
    assert call.get_container().get_password() is None
    assert call.get_container().get_username() is None


def test_env_variables(lzy):
    @op
    def foo() -> None:
        pass

    # TODO: change with helper
    foo = foo.with_env_vars(a="a1", b="b1")

    with lzy.workflow("test").with_env_vars({'b': 'b2', 'c': 'c2'}) as wf:
        foo()

    call: LzyCall = wf.first_call
    assert call.get_env_vars() == {'a': 'a1', 'b': 'b1', 'c': 'c2'}


def test_op_with_default_cache_params(lzy):
    default_version = "0.0"

    @op
    def cached_op(a: int) -> str:
        return f"Value: {a}"

    with lzy.workflow("test") as wf:
        cached_op(42)

    call: LzyCall = wf.first_call
    assert call.cache is False
    assert default_version == call.version


def test_cached_op(lzy):
    version = "1.0"

    @op(cache=True, version=version)
    def cached_op(a: int) -> str:
        return f"Value: {a}"

    with lzy.workflow("test") as wf:
        cached_op(42)

    call: LzyCall = wf.first_call
    assert call.cache is True
    assert version == call.version
