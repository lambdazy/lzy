from __future__ import annotations

from enum import Enum
from dataclasses import dataclass
from typing import Optional

from .base import BaseContainer


class DockerPullPolicy(Enum):
    ALWAYS = "ALWAYS"  # Always pull the newest version of image
    IF_NOT_EXISTS = "IF_NOT_EXISTS"  # Pull image once and cache it for next executions


@dataclass
class DockerContainer(BaseContainer):
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
