from __future__ import annotations

import inspect

from typing import Optional

from .base import NotSpecified
from .environment import EnvVarsType
from .provisioning.provisioning import ProvisioningRequirement
from .mixin import WithEnvironmentMixin, WithEnvironmentType, EnvironmentApplierType
from .container.docker import DockerPullPolicy
from .python.base import ModulePathsList, PackagesDict


def assert_for_environment(subject: WithEnvironmentType) -> None:
    if isinstance(subject, WithEnvironmentMixin):
        return

    frame_info = inspect.stack()[2]  # type: ignore[unreachable]
    function_name = frame_info.function

    raise TypeError(
        f'decorator/function {function_name} can be applied only to '
        f'@op-decorated functions, Lzy and LzyWorkflow objects'
    )


def manual_python_env(
    *,
    python_version: str,
    local_module_paths: ModulePathsList,
    pypi_packages: PackagesDict,
    pypi_index_url: Optional[str] = None
) -> EnvironmentApplierType:
    def wrapper(subject: WithEnvironmentType) -> WithEnvironmentType:
        assert_for_environment(subject)

        return subject.with_manual_python_env(  # type: ignore[no-any-return]
            python_version=python_version,
            local_module_paths=local_module_paths,
            pypi_packages=pypi_packages,
            pypi_index_url=pypi_index_url
        )

    return wrapper


def auto_python_env(
    *,
    pypi_index_url: Optional[str] = None,
    additional_pypi_packages: Optional[PackagesDict] = None,
) -> EnvironmentApplierType:
    def wrapper(subject: WithEnvironmentType) -> WithEnvironmentType:
        assert_for_environment(subject)

        return subject.with_auto_python_env(
            pypi_index_url=pypi_index_url,
            additional_pypi_packages=additional_pypi_packages,
        )

    return wrapper


def docker_container(
    *,
    registry: str,
    image: str,
    pull_policy: DockerPullPolicy = DockerPullPolicy.IF_NOT_EXISTS,
    username: Optional[str] = None,
    password: Optional[str] = None,
) -> EnvironmentApplierType:
    def wrapper(subject: WithEnvironmentType) -> WithEnvironmentType:
        assert_for_environment(subject)

        return subject.with_docker_container(
            registry=registry,
            image=image,
            pull_policy=pull_policy,
            username=username,
            password=password,
        )

    return wrapper


def no_container() -> EnvironmentApplierType:
    def wrapper(subject: WithEnvironmentType) -> WithEnvironmentType:
        assert_for_environment(subject)

        return subject.with_no_container()

    return wrapper


def provisioning(
    *,
    cpu_type: ProvisioningRequirement[str] = NotSpecified,
    cpu_count: ProvisioningRequirement[int] = NotSpecified,
    gpu_type: ProvisioningRequirement[str] = NotSpecified,
    gpu_count: ProvisioningRequirement[int] = NotSpecified,
    ram_size_gb: ProvisioningRequirement[int] = NotSpecified,
) -> EnvironmentApplierType:
    def wrapper(subject: WithEnvironmentType) -> WithEnvironmentType:
        assert_for_environment(subject)

        return subject.with_provisioning(
            cpu_type=cpu_type,
            cpu_count=cpu_count,
            gpu_type=gpu_type,
            gpu_count=gpu_count,
            ram_size_gb=ram_size_gb,
        )

    return wrapper


def env_vars(**kwargs: EnvVarsType) -> EnvironmentApplierType:
    def wrapper(subject: WithEnvironmentType) -> WithEnvironmentType:
        assert_for_environment(subject)

        return subject.with_env_vars(**kwargs)

    return wrapper
