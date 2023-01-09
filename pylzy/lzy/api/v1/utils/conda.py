import warnings
from typing import Any, Dict, List, cast

import yaml

from lzy.logs.config import get_logger

_LOG = get_logger(__name__)

# TODO(tomato): rethink this cache
_installed_versions = {"3.7.11": "py37", "3.8.12": "py38", "3.9.7": "py39"}


def generate_conda_yaml(python_version: str, installed_packages: Dict[str, str]) -> str:
    if python_version in _installed_versions:
        name = _installed_versions[python_version]
        deps: List[Any] = []
    else:
        warnings.warn(f"Installed python version ({python_version}) is not cached remotely. "
                      f"Usage of a cached python version ({list(_installed_versions.keys())})"
                      f" can decrease startup time.")
        deps = [f"python=={python_version}"]
        name = "default"

    deps.append("pip")
    deps.append(
        {"pip": [f"{name}=={version}" for name, version in installed_packages.items()]}
    )

    conda_yaml = {"name": name, "dependencies": deps}
    return cast(str, yaml.dump(conda_yaml, sort_keys=False))
