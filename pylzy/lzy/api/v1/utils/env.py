from pathlib import Path
from typing import Dict, Optional, Sequence

import yaml

from lzy.api.v1.env import CondaEnv, DockerEnv, DockerPullPolicy, Env
from lzy.api.v1.utils.conda import generate_conda_yaml, merge_conda_env_yamls
from lzy.py_env.api import PyEnv


def generate_env(
    py_env: PyEnv,
    python_version: Optional[str] = None,
    libraries: Optional[Dict[str, str]] = None,
    conda_yaml_path: Optional[str] = None,
    docker_image: Optional[str] = None,
    docker_pull_policy: DockerPullPolicy = DockerPullPolicy.IF_NOT_EXISTS,
    local_modules_path: Optional[Sequence[str]] = None,
) -> Env:
    conda_env: CondaEnv
    if conda_yaml_path is not None:
        yaml_file = Path(conda_yaml_path)
        if not yaml_file.is_file():
            raise ValueError("Specified conda yaml file does not exists")
        with yaml_file.open("r") as f:
            yaml_content = f.read()
        conda_env = CondaEnv(yaml_content)
    elif python_version is not None and libraries is not None:
        conda_env = CondaEnv(generate_conda_yaml(python_version, libraries))
    elif python_version is not None:
        conda_env = CondaEnv(generate_conda_yaml(python_version, py_env.libraries))
    elif libraries is not None:
        conda_env = CondaEnv(generate_conda_yaml(py_env.python_version, libraries))
    else:
        conda_env = CondaEnv(
            generate_conda_yaml(py_env.python_version, py_env.libraries)
        )

    docker_env: Optional[DockerEnv] = None
    if docker_image is not None:
        docker_env = DockerEnv(docker_image, docker_pull_policy)

    if local_modules_path is None:
        local_modules_path = py_env.local_modules_path

    return Env(conda_env, local_modules_path, docker_env)


def merge_envs(primary: Env, secondary: Env) -> Env:
    docker_env = primary.docker if primary.docker is not None else secondary.docker
    conda_env = merge_conda_envs(primary.conda, secondary.conda)
    modules = set(primary.local_modules).union(secondary.local_modules)
    return Env(conda_env, list(modules), docker_env)


def merge_conda_envs(primary: CondaEnv, secondary: CondaEnv) -> CondaEnv:
    return CondaEnv(merge_conda_env_yamls([secondary.yaml, primary.yaml]))
