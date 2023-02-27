from dataclasses import dataclass, field
from enum import Enum
from typing import Optional, Sequence, Dict, Mapping, Set


class DockerPullPolicy(Enum):
    ALWAYS = "ALWAYS"  # Always pull the newest version of image
    IF_NOT_EXISTS = "IF_NOT_EXISTS"  # Pull image once and cache it for next executions


@dataclass
class DockerCredentials:
    registry: str
    username: str
    password: str


@dataclass
class Env:
    python_version: Optional[str] = None
    libraries: Dict[str, str] = field(default_factory=dict)
    conda_yaml_path: Optional[str] = None
    docker_image: Optional[str] = None
    docker_pull_policy: Optional[DockerPullPolicy] = None
    local_modules_path: Optional[Sequence[str]] = None
    docker_only: bool = False
    env_variables: Mapping[str, str] = field(default_factory=dict)
    docker_credentials: Optional[DockerCredentials] = None

    def override(self, other: "Env") -> "Env":
        local_modules: Set[str] = set()
        if self.local_modules_path:
            local_modules.update(self.local_modules_path)

        if other.local_modules_path:
            local_modules.update(other.local_modules_path)

        return Env(
            python_version=other.python_version if other.python_version else self.python_version,
            libraries={**self.libraries, **other.libraries},
            conda_yaml_path=other.conda_yaml_path if other.conda_yaml_path else self.conda_yaml_path,
            docker_image=other.docker_image if other.docker_image else self.docker_image,
            docker_pull_policy=other.docker_pull_policy if other.docker_pull_policy else self.docker_pull_policy,
            local_modules_path=[*local_modules],
            docker_only=self.docker_only or other.docker_only,
            env_variables={**self.env_variables, **other.env_variables},
            docker_credentials=other.docker_credentials if other.docker_credentials else self.docker_credentials
        )

    def validate(self) -> None:
        if self.conda_yaml_path and (self.python_version or self.libraries):
            raise ValueError("Python version & libraries cannot be overriden if conda yaml is set. "
                             "You can specify them inside the yaml file.")

        if self.docker_image and not self.docker_pull_policy:
            raise ValueError("docker_image is set but docker_pull_policy is not")

        if self.local_modules_path is None:
            raise ValueError("local_modules_path is not set")

        if self.docker_only and self.docker_image is None:
            raise ValueError("docker_only is set, but docker image is not set")
