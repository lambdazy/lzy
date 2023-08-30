from __future__ import annotations

from typing import TypeVar, Optional
from typing_extensions import Self

from lzy.utils.functools import kwargsdispatchmethod
from .base import Deconstructible, NotSpecified
from .environment import LzyEnvironment, EnvVarsType
from .container.base import BaseContainer
from .provisioning.provisioning import Provisioning, ProvisioningRequirement
from .python.base import BasePythonEnv, ModulePathsList, PackagesDict
from .python.manual import ManualPythonEnv


class WithEnvironmentMixin(Deconstructible):
    env: LzyEnvironment

    def with_env(self, env: LzyEnvironment) -> Self:
        return self.with_fields(env=env)

    def with_env_vars(self, env_vars: EnvVarsType) -> Self:
        return self.with_env(
            self.env.with_fields(env_vars=env_vars)
        )

    @kwargsdispatchmethod
    def with_provisioning(self, *args, **kwargs) -> Self:
        raise NotImplementedError('wrong argument types')

    @with_provisioning.register('args')
    def _(self, provisioning: Provisioning) -> Self:
        if not isinstance(provisioning, Provisioning):
            raise TypeError('bad argument types')

        return self.with_env(
            self.env.with_fields(provisioning=provisioning)
        )

    @with_provisioning.register('kwargs')
    def _(
        self,
        *,
        cpu_type: ProvisioningRequirement[str] = NotSpecified,
        cpu_count: ProvisioningRequirement[int] = NotSpecified,
        gpu_type: ProvisioningRequirement[str] = NotSpecified,
        gpu_count: ProvisioningRequirement[int] = NotSpecified,
        ram_size_gb: ProvisioningRequirement[int] = NotSpecified,
    ) -> Self:
        provisioning = Provisioning(
            cpu_type=cpu_type,
            cpu_count=cpu_count,
            gpu_type=gpu_type,
            gpu_count=gpu_count,
            ram_size_gb=ram_size_gb
        )
        return self.with_env(
            self.env.with_fields(provisioning=provisioning)
        )

    def with_container(self, container: BaseContainer) -> Self:
        return self.with_env(
            self.env.with_fields(container=container)
        )

    def with_python_env(self, python_env: BasePythonEnv) -> Self:
        return self.with_env(
            self.env.with_fields(python_env=python_env)
        )

    def with_manual_python_env(
        self,
        *,
        python_version: str,
        local_module_paths: ModulePathsList,
        pypi_packages: PackagesDict,
        pypi_index_url: Optional[str] = None,
    ):
        return self.with_python_env(
            ManualPythonEnv(
                python_version=python_version,
                local_module_paths=local_module_paths,
                pypi_packages=pypi_packages,
                pypi_index_url=pypi_index_url
            )
        )


WithEnvironmentType = TypeVar('WithEnvironmentType', bound=WithEnvironmentMixin)
