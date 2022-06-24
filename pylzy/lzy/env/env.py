from dataclasses import dataclass
from typing import List, Optional, Union


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
    base_env: BaseEnv = BaseEnv("default")
    aux_env: Optional[AuxEnv] = None
