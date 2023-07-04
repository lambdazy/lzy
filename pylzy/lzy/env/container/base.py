from __future__ import annotations

from abc import ABCMeta, abstractmethod
from enum import Enum
from typing import Optional, ClassVar, TYPE_CHECKING
from typing_extensions import Protocol, runtime_checkable

from lzy.env.base import Deconstructible

if TYPE_CHECKING:
    from lzy.env.mixin import WithEnvironmentType


class DockerPullPolicy(Enum):
    ALWAYS = "ALWAYS"  # Always pull the newest version of image
    IF_NOT_EXISTS = "IF_NOT_EXISTS"  # Pull image once and cache it for next executions


@runtime_checkable
class DockerProtocol(Protocol):
    @abstractmethod
    def get_image_url(self) -> str:
        raise NotImplementedError

    @abstractmethod
    def get_pull_policy(self) -> DockerPullPolicy:
        raise NotImplementedError

    @abstractmethod
    def get_username(self) -> Optional[str]:
        raise NotImplementedError

    @abstractmethod
    def get_password(self) -> Optional[str]:
        raise NotImplementedError


@runtime_checkable
class NoContainer(Protocol):
    pass


class SupportedContainerTypes(Enum):
    NoContainer = NoContainer
    Docker = DockerProtocol


class ContainerMeta(ABCMeta):
    def __init__(cls, name, bases, nmspc) -> None:
        super().__init__(name, bases, nmspc)

        if len(bases) == 1 and Deconstructible in bases and name == 'BaseContainer':
            return

        container_type: Optional[SupportedContainerTypes] = getattr(cls, 'container_type', None)

        if not container_type or not isinstance(container_type, SupportedContainerTypes):
            raise TypeError(
                'all of BaseContainer subclasses must have container_type attribute '
                f'with {SupportedContainerTypes} type'
            )

        expected_protocol = container_type.value

        if issubclass(cls, expected_protocol):
            return

        raise TypeError(
            f'class with container_type=={container_type} '
            f'must be realization of {expected_protocol}'
        )


class BaseContainer(Deconstructible, metaclass=ContainerMeta):
    container_type: ClassVar[SupportedContainerTypes]

    def __call__(self, subject: WithEnvironmentType) -> WithEnvironmentType:
        return subject.with_container(self)
