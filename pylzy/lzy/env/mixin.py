from __future__ import annotations

from typing import TypeVar, Optional
from typing_extensions import Self

from lzy.utils.functools import kwargsdispatchmethod
from .base import Deconstructible
from .environment import LzyEnvironment, EnvVarsType
from .container.base import BaseContainer
from .provisioning.provisioning import Provisioning
from .python.base import BasePythonEnv


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

    @with_provisioning.register(type='args')
    def _(self, provisioning: Provisioning) -> Self:
        if not isinstance(provisioning, Provisioning):
            raise TypeError('bad argument types')

        return self.with_env(
            self.env.with_fields(provisioning=provisioning)
        )

    @with_provisioning.register(type='kwargs')
    def _(
        self,
        *,
        provisioning_class: Type[Provisioning] = Provisioning,
        cpu_type: Optional[str] = None,
        cpu_count: Optional[int] = None,
        gpu_type: Optional[str] = None,
        gpu_count: Optional[int] = None,
        ram_size_gb: Optional[int] = None,
    ) -> Self:
        kwargs = {k: v for k, v in {
            'cpu_type': cpu_type,
            'cpu_count': cpu_count,
            'gpu_type': gpu_type,
            'gpu_count': gpu_count,
            'ram_size_gb': ram_size_gb,
        }.items() if v is not None}
        provisioning = provisioning_class(**kwargs)
        return self.with_provisioning(provisioning)

    def with_container(self, container: BaseContainer) -> Self:
        return self.with_env(
            self.env.with_fields(container=container)
        )

    def with_python_env(self, python_env: BasePythonEnv) -> Self:
        return self.with_env(
            self.env.with_fields(python_env=python_env)
        )


WithEnvironmentType = TypeVar('WithEnvironmentType', bound=WithEnvironmentMixin)
