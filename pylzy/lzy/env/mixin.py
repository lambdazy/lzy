from __future__ import annotations

from dataclasses import dataclass
from typing import TypeVar
from typing_extensions import Self

from .base import Deconstructible
from .environment import LzyEnvironment
from .container.base import BaseContainer
from .provisioning.provisioning import Provisioning
from .python.base import BasePythonEnv


@dataclass
class WithEnvironmentMixin(Deconstructible):
    env: LzyEnvironment

    def with_env(self, env: LzyEnvironment) -> Self:
        return self.with_fields(env=env)

    def with_provisioning(self, provisioning: Provisioning) -> Self:
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


WithEnvironmentType = TypeVar('WithEnvironmentType', bound=WithEnvironmentMixin)
