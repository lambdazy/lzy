from dataclasses import dataclass
from enum import Enum
from typing import Optional, Sequence


class DockerPullPolicy(Enum):
    ALWAYS = "ALWAYS"  # Always pull the newest version of image
    IF_NOT_EXISTS = "IF_NOT_EXISTS"  # Pull image once and cache it for next executions


@dataclass
class CondaEnv:
    yaml: str


@dataclass
class DockerEnv:
    image: str
    pull_policy: DockerPullPolicy


@dataclass
class Env:
    conda: CondaEnv
    local_modules: Sequence[str]
    docker: Optional[DockerEnv] = None
