# pylint: disable=too-many-instance-attributes

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, TypeVar, Callable, TYPE_CHECKING
from typing_extensions import Self, final

from .base import Deconstructible, NotSpecified, EnvironmentField, is_specified
from .container.base import BaseContainer
from .container.no_container import NoContainer
from .provisioning.provisioning import Provisioning
from .python.auto import AutoPythonEnv
from .python.base import BasePythonEnv

if TYPE_CHECKING:
    from .mixin import WithEnvironmentType


T = TypeVar('T')
DefaultFactory = Callable[[], T]
EnvironType = Dict[str, str]


@final
@dataclass
class LzyEnvironment(Deconstructible):
    environ: EnvironmentField[EnvironType] = NotSpecified
    _environ_default_factory: DefaultFactory[EnvironType] = field(
        default=dict, init=False
    )

    provisioning: EnvironmentField[Provisioning] = NotSpecified
    _provisioning_default_factory: DefaultFactory[Provisioning] = field(
        default=Provisioning, init=False
    )

    python_env: EnvironmentField[BasePythonEnv] = NotSpecified
    _python_env_default_factory: DefaultFactory[BasePythonEnv] = field(
        default=AutoPythonEnv, init=False
    )

    container: EnvironmentField[BaseContainer] = NotSpecified
    _container_default_factory: DefaultFactory[BaseContainer] = field(
        default=NoContainer, init=False
    )

    def __call__(self, subject: WithEnvironmentType) -> WithEnvironmentType:
        return subject.with_env(self)

    def with_fields(
        self,
        environ: EnvironmentField[EnvironType] = NotSpecified,
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
            new_provisioning = self.provisioning or provisioning

        return super().with_fields(
            environ=self.environ or environ,
            provisioning=new_provisioning,
            python_env=self.python_env or python_env,
            container=self.container or container,
            **kwargs,
        )

    # get_* methods are meant to be used after all LzyEnvironment merging
    # at runtime code
    def get_environ(self) -> EnvironType:
        if is_specified(self.environ):
            return self.environ
        return self._environ_default_factory()

    def get_provisioning(self) -> Provisioning:
        if is_specified(self.provisioning):
            return self.provisioning
        return self._provisioning_default_factory()

    def get_python_env(self) -> BasePythonEnv:
        if is_specified(self.python_env):
            return self.python_env
        return self._python_env_default_factory()

    def get_container(self) -> BaseContainer:
        if is_specified(self.container):
            return self.container
        return self._container_default_factory()
