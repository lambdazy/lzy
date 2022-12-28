from dataclasses import dataclass, field
from enum import Enum
from typing import Optional, Sequence, Dict


class DockerPullPolicy(Enum):
    ALWAYS = "ALWAYS"  # Always pull the newest version of image
    IF_NOT_EXISTS = "IF_NOT_EXISTS"  # Pull image once and cache it for next executions


@dataclass
class Env:
    python_version: Optional[str] = None
    libraries: Dict[str, str] = field(default_factory=dict)
    conda_yaml_path: Optional[str] = None
    docker_image: Optional[str] = None
    docker_pull_policy: Optional[DockerPullPolicy] = None
    local_modules_path: Optional[Sequence[str]] = None

    def override(self, other: "Env") -> "Env":
        return Env(
            python_version=other.python_version if other.python_version else self.python_version,
            libraries={**self.libraries, **other.libraries},
            conda_yaml_path=other.conda_yaml_path if other.conda_yaml_path else self.conda_yaml_path,
            docker_image=other.docker_image if other.docker_image else self.docker_image,
            docker_pull_policy=other.docker_pull_policy if other.docker_pull_policy else self.docker_pull_policy,
            local_modules_path=other.local_modules_path if other.local_modules_path is not None else
            self.local_modules_path
        )

    def validate(self) -> None:
        if self.conda_yaml_path and (self.python_version or self.libraries):
            raise ValueError("Python version & libraries cannot be overriden if conda yaml is set. "
                             "You can specify them inside the yaml file.")

        if self.docker_image and not self.docker_pull_policy:
            raise ValueError("docker_image is set but docker_pull_policy is not")

        if self.local_modules_path is None:
            raise ValueError("local_modules_path is not set")
