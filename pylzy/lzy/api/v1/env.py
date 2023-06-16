import dataclasses
import pathlib
from enum import Enum
from typing import (
    Any,
    Optional,
    Sequence,
    Dict,
    Mapping,
    List,
)

import yaml

from pypi_simple import PYPI_SIMPLE_ENDPOINT

from lzy.py_env.api import PyEnv
from lzy.logs.config import get_logger
from lzy.utils.pypi import validate_pypi_index_url

_LOG = get_logger(__name__)

# TODO(tomato): rethink this cache
_INSTALLED_VERSIONS = {"3.7.11": "py37", "3.8.12": "py38", "3.9.7": "py39"}


class DockerPullPolicy(Enum):
    ALWAYS = "ALWAYS"  # Always pull the newest version of image
    IF_NOT_EXISTS = "IF_NOT_EXISTS"  # Pull image once and cache it for next executions


@dataclasses.dataclass
class DockerCredentials:
    registry: str
    username: str
    password: str


@dataclasses.dataclass
class Env:
    env_variables: Mapping[str, str] = dataclasses.field(default_factory=dict)

    python_version: Optional[str] = None
    libraries: Dict[str, str] = dataclasses.field(default_factory=dict)
    local_modules_path: Sequence[str] = dataclasses.field(default_factory=list)

    pypi_index_url: Optional[str] = None

    conda_yaml_path: Optional[str] = None

    docker_image: Optional[str] = None
    docker_pull_policy: Optional[DockerPullPolicy] = None
    docker_credentials: Optional[DockerCredentials] = None

    def override(
        self,
        other: Optional["Env"] = None,
        **kwargs,
    ) -> "Env":
        if other and kwargs:
            raise TypeError('usage of args and kwargs at the same time is forbidden')

        if other:
            kwargs = dataclasses.asdict(other)

        # merging arguments which have to be merged
        kwargs['libraries'] = {**self.libraries, **(kwargs.get('libraries') or {})}
        kwargs['env_variables'] = {**self.env_variables, **(kwargs.get('env_variables') or {})}
        kwargs['local_modules_path'] = list(
            set(self.local_modules_path) |
            set(kwargs.get('local_modules_path') or set())
        )

        new_kwargs = {}
        for field in dataclasses.fields(self):
            name = field.name
            value = kwargs.pop(name, None)

            # NB: Here we are treating None as absense of rvalue
            if value is None:
                value = getattr(self, name)

            new_kwargs[name] = value

        if kwargs:
            extra_args = ', '.join(kwargs)
            raise TypeError(f"got an unexpected keyword arguments '{extra_args}'")

        return dataclasses.replace(self, **new_kwargs)

    def validate(self) -> "Env":
        if self.conda_yaml_path and (self.python_version or self.libraries):
            raise ValueError(
                "Python version & libraries cannot be overriden if conda yaml is set. "
                "You can specify them inside the yaml file."
            )

        if self.docker_image and not self.docker_pull_policy:
            raise ValueError("docker_image is set but docker_pull_policy is not")

        if self.pypi_index_url:
            validate_pypi_index_url(self.pypi_index_url)

        return self

    def finalize(self, op_env: "Env", py_env: PyEnv) -> "Env":
        """
        Here happens final merge of all data we collect about env:

        1) Env from .workflow
        2) Env from @op
        3) And info about local packages from PyEnv

        This method supposed to be called at @op's lazy wrapper when
        we finally collect all 1, 2 and 3 env sources.

        Note, that in this method:
        1) Env from .workflow is self
        2) Env from @op is op_env
        3) PyEnv is py_env

        """

        merged = self.override(op_env)

        # in any case we need to send local modules from PyEnv to server
        # because it represents local script at minimum.
        # XXX: it could also include for example strange deb-libraries
        # which are not located at PyPi and we decided that it is "local"
        local_modules_path = list(
            set(merged.local_modules_path) |
            set(py_env.local_modules_path)
        )

        # we respect manual python version which user set via Env and in such case
        # we are dropping automaticly inferred version
        python_version = merged.python_version

        # if user using conda_yaml_path
        # we must to send only libraries manually setted via Env,
        # and add automaticly inferred libraries otherwise
        is_manual = bool(merged.conda_yaml_path)
        libraries = merged.libraries.copy()
        if not is_manual:
            libraries.update(py_env.libraries)
            python_version = python_version or py_env.python_version

        return merged.override(
            python_version=python_version,
            local_modules_path=local_modules_path,
            libraries=libraries,
        ).validate()

    def generate_conda_config(self) -> Dict[str, Any]:
        python_version = self.python_version
        dependencies: List[Any] = []

        if python_version in _INSTALLED_VERSIONS:
            env_name = _INSTALLED_VERSIONS[python_version]
        else:
            _LOG.warn(
                f"Installed python version ({python_version}) is not cached remotely. "
                f"Usage of a cached python version ({list(_INSTALLED_VERSIONS.keys())}) "
                f"can decrease startup time."
            )
            dependencies = [f"python=={python_version}"]
            env_name = "default"

        dependencies.append("pip")

        extra_pip_options = []
        if self.pypi_index_url:
            extra_pip_options.append(f'--index-url {self.pypi_index_url}')

        libraries = [f"{name}=={version}" for name, version in self.libraries.items()]

        pip_options = extra_pip_options + libraries
        if pip_options:
            dependencies.append({"pip": pip_options})

        return {"name": env_name, "dependencies": dependencies}

    def get_conda_yaml(self) -> str:
        if self.conda_yaml_path:
            path = pathlib.Path(self.conda_yaml_path)
            result = path.read_text(encoding="utf-8")
        else:
            config = self.generate_conda_config()
            result = yaml.dump(config, sort_keys=False)

        return result
