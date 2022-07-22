from dataclasses import dataclass
from enum import Enum
from typing import List, Optional, Union


class ImagePullPolicy(Enum):
    ALWAYS = "ALWAYS"                # Always pull the newest version of image
    IF_NOT_EXISTS = "IF_NOT_EXISTS"  # Pull image once and cache it for next executions


@dataclass
class BaseEnv:
    name: str


@dataclass
class AuxEnv:
    name: str
    conda_yaml: str
    local_modules_paths: List[str]


@dataclass
class EnvSpec:
    base_env: BaseEnv
    aux_env: AuxEnv
