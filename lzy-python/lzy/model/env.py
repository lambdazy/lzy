from abc import ABC, abstractmethod
import logging
from types import ModuleType
from typing import Dict, Tuple, Iterable


class Env(ABC):
    # @abstractmethod
    # def name(self) -> str:
    #     pass

    # probably it's better to declare to_json method
    # but it doesn't work with google protobuf json parser
    # and return type probably is not the best choice but ok for now
    @abstractmethod
    def as_dct(self) -> Dict[str, str]:
        pass

    @abstractmethod
    def type_id(self) -> str:
        pass


class PyEnv(Env):
    def __init__(self, env_name: str, yaml: str, local_packages: Iterable[ModuleType], local_modules_uploaded):
        super().__init__()
        self._name = env_name
        self._yaml = yaml
        self._local_packages = local_packages
        self._log = logging.getLogger(str(self.__class__))
        self._local_modules_uploaded = local_modules_uploaded

    def type_id(self) -> str:
        return "pyenv"

    def name(self) -> str:
        return self._name

    def local_modules(self) -> Iterable[ModuleType]:
        return self._local_packages

    def yaml(self) -> str:
        return self._yaml

    def local_modules_uploaded(self):
        return self._local_modules_uploaded

    def as_dct(self):
        if self._local_modules_uploaded:
            return {"name": self._name, "yaml": self._yaml,
                    "localModules": [{"name": tuple[0], "uri": tuple[1]} for index, tuple
                                     in enumerate(self._local_modules_uploaded)]}
        else:
            return {"name": self._name, "yaml": self._yaml}


PACKAGES_DELIM = ";"
PACKAGE_VERS_DELIM = "=="


def to_str_version(version: Tuple[str]) -> str:
    return ".".join(version)


def to_str_packages(packages: Dict[str, Tuple[str]]) -> str:
    return join_packages((n, ".".join(v)) for n, v in packages.items())


def join_packages(packages: Iterable[Tuple[str, str]]) -> str:
    return PACKAGES_DELIM.join(
        PACKAGE_VERS_DELIM.join(pack_info) for pack_info in packages
    )


def parse_version(version: str) -> Tuple[str, ...]:
    return tuple(version.split("."))
