# pylint: disable=too-many-instance-attributes

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, TypeVar, TYPE_CHECKING
from typing_extensions import Self, final

from .base import Deconstructible, NotSpecified, EnvironmentField, is_specified
from .container.base import BaseContainer
from .container.no_container import NoContainer
from .provisioning.provisioning import Provisioning
from .python.auto import AutoPythonEnv
from .python.base import BasePythonEnv, NamespaceType

if TYPE_CHECKING:
    from .mixin import WithEnvironmentType


T = TypeVar('T')
EnvVarsType = Dict[str, str]


@final
@dataclass
class LzyEnvironment(Deconstructible):
    env_vars: EnvironmentField[EnvVarsType] = NotSpecified
    provisioning: EnvironmentField[Provisioning] = NotSpecified
    python_env: EnvironmentField[BasePythonEnv] = NotSpecified
    container: EnvironmentField[BaseContainer] = NotSpecified
    namespace: EnvironmentField[NamespaceType] = NotSpecified

    def __call__(self, subject: WithEnvironmentType) -> WithEnvironmentType:
        return subject.with_env(self)

    def with_fields(
        self,
        env_vars: EnvironmentField[EnvVarsType] = NotSpecified,
        provisioning: EnvironmentField[Provisioning] = NotSpecified,
        python_env: EnvironmentField[BasePythonEnv] = NotSpecified,
        container: EnvironmentField[BaseContainer] = NotSpecified,
        **kwargs,  # kwargs needed for compatibility with base class
    ) -> Self:
        # method overrided only to combine provisioning
        new_provisioning: EnvironmentField[Provisioning]
        if is_specified(self.provisioning) and is_specified(provisioning):
            new_provisioning = self.provisioning.combine(provisioning)
        else:
            new_provisioning = provisioning or self.provisioning

        new_env_vars: EnvironmentField[EnvVarsType]
        if is_specified(self.env_vars) and is_specified(env_vars):
            new_env_vars = {**self.env_vars, **env_vars}
        else:
            new_env_vars = env_vars or self.env_vars

        return super().with_fields(
            env_vars=new_env_vars,
            provisioning=new_provisioning,
            python_env=python_env or self.python_env,
            container=container or self.container,
            **kwargs,
        )

    # get_* methods are meant to be used after all LzyEnvironment merging
    # at runtime code
    def get_env_vars(self) -> EnvVarsType:
        if is_specified(self.env_vars):
            return self.env_vars
        return {}

    def get_provisioning(self) -> Provisioning:
        if is_specified(self.provisioning):
            return self.provisioning
        return Provisioning()

    def get_python_env(self) -> BasePythonEnv:
        if is_specified(self.python_env):
            return self.python_env
        return AutoPythonEnv()

    def get_container(self) -> BaseContainer:
        if is_specified(self.container):
            return self.container
        return NoContainer()

    def get_namespace(self) -> NamespaceType:
        if is_specified(self.namespace):
            return self.namespace
        return {}

    def validate(self) -> None:
        self.get_python_env().validate()
        self.get_provisioning().validate()
