from typing import Any, Dict, List, cast

import yaml

# TODO(tomato): rethink this cache
_installed_versions = {"3.7.11": "py37", "3.8.12": "py38", "3.9.7": "py39"}


def generate_conda_yaml(python_version: str, installed_packages: Dict[str, str], name: str = "default") -> str:
    if python_version in _installed_versions:
        name = _installed_versions[python_version]
        deps: List[Any] = []
    else:
        deps = [f"python=={python_version}"]

    deps.append("pip")
    deps.append(
        {"pip": [f"{name}=={version}" for name, version in installed_packages.items()]}
    )

    conda_yaml = {"name": name, "dependencies": deps}
    return cast(str, yaml.dump(conda_yaml, sort_keys=False))
