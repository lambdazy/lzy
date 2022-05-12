from dataclasses import dataclass
from typing import Optional, List


@dataclass
class BaseEnv:
    base_docker_image: str


@dataclass
class AuxEnv:
    name: str
    conda_yaml: str
    local_modules_paths: List[str]


@dataclass
class Env:
    base_env: Optional[BaseEnv] = None
    aux_env: Optional[AuxEnv] = None
