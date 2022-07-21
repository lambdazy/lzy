import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import yaml
import logging
from yaml import safe_load

from lzy.api.v2.servant.model.encoding import ENCODING as encoding
from lzy.env.env import *
from lzy.env.env_provider import EnvProvider
from lzy.pkg_info import (
    _installed_versions,
    all_installed_packages,
    select_modules,
    to_str,
)


def create_yaml(
    installed_packages: Dict[str, Tuple[str, ...]], name: str = "default"
) -> Tuple[str, str]:
    # always use only first three numbers, otherwise conda won't find
    python_version = to_str(sys.version_info[:3])
    if python_version in _installed_versions:
        name = _installed_versions[python_version]
        deps: List[Any] = []
    else:
        deps = [f"python=={python_version}"]

    deps.append("pip")
    deps.append(
        {
            "pip": [
                f"{name}=={to_str(version)}"
                for name, version in installed_packages.items()
            ]
        }
    )

    conda_yaml = {"name": name, "dependencies": deps}
    return name, yaml.dump(conda_yaml, sort_keys=False)


class LzyEnvProvider(EnvProvider):
    def __init__(
        self,
        conda_yaml_path: Optional[Path] = None,
        local_modules_paths: Optional[List[str]] = None,
        image_name: str = "default",
        image_pull_policy: ImagePullPolicy = ImagePullPolicy.ALWAYS,
    ):
        self._log = logging.getLogger(str(self.__class__))

        self._conda_yaml_path = conda_yaml_path
        self._local_modules_paths = local_modules_paths
        self._image_name = image_name
        if image_pull_policy != ImagePullPolicy.ALWAYS:
            self._log.warning(
                f"Only ImagePullPolicy.ALWAYS is supported now, ignore ImagePullPolicy {image_pull_policy}"
            )
        self._image_pull_policy = ImagePullPolicy.ALWAYS

    def provide(self, namespace: Optional[Dict[str, Any]] = None) -> EnvSpec:
        local_module_paths_found: List[str] = []
        if self._conda_yaml_path is None:
            if namespace is None:
                name, yaml = create_yaml(installed_packages=all_installed_packages())
            else:
                installed, local_module_paths_found = select_modules(namespace)
                name, yaml = create_yaml(installed_packages=installed)

            if self._local_modules_paths is None:
                local_modules_paths = local_module_paths_found
            else:
                local_modules_paths = self._local_modules_paths

            return EnvSpec(
                base_env=BaseEnv(name=self._image_name),
                aux_env=AuxEnv(
                    name=name, conda_yaml=yaml, local_modules_paths=local_modules_paths
                )
            )

        # TODO: as usually not good idea to read whole file into memory
        # TODO: but right now it's the best option
        with open(self._conda_yaml_path, "r", encoding=encoding) as file:
            name, yaml_str = "default", file.read()
            data = safe_load(yaml_str)
            return EnvSpec(
                base_env=BaseEnv(name=self._image_name),
                aux_env=AuxEnv(
                    name=data.get("name", name),
                    conda_yaml=yaml_str,
                    local_modules_paths=[],
                )
            )
