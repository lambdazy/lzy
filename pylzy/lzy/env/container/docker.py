from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, ClassVar

from .base import BaseContainer, DockerPullPolicy, ContainerTypes


@dataclass
class DockerContainer(BaseContainer):
    container_type: ClassVar[ContainerTypes] = ContainerTypes.Docker

    image_url: str
    pull_policy: DockerPullPolicy = DockerPullPolicy.IF_NOT_EXISTS
    username: Optional[str] = None
    password: Optional[str] = None

    def get_image_url(self) -> str:
        return self.image_url

    def get_pull_policy(self) -> DockerPullPolicy:
        return self.pull_policy

    def get_username(self) -> Optional[str]:
        return self.username

    def get_password(self) -> Optional[str]:
        return self.password
