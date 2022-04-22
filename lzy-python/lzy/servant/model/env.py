import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Dict, Optional, Tuple, Iterable, List


class BaseEnv(ABC):

    def __init__(self, name: str = ""):
        super().__init__()
        self._name = name

    def name(self) -> str:
        return self._name

    def as_dct(self) -> Dict[str, str]:
        return {"name": self._name}


class AuxEnv(ABC):

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


class PyEnv(AuxEnv):
    def __init__(self, env_name: str, yaml: str, local_modules_uploaded: List[Tuple[str, str]]):
        super().__init__()
        self._name = env_name
        self._yaml = yaml
        self._log = logging.getLogger(str(self.__class__))
        self._local_modules_uploaded = local_modules_uploaded

    def type_id(self) -> str:
        return "pyenv"

    def name(self) -> str:
        return self._name

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


@dataclass
class EnvDataclass():
    base_env: Optional[BaseEnv] = None
    aux_env: Optional[AuxEnv] = None


class Env(EnvDataclass, ABC):

    def as_dct(self):
        dct = {}
        if self.base_env:
            dct["baseEnv"] = self.base_env.as_dct()
        if self.aux_env:
            dct["auxEnv"] = {self.aux_env.type_id() : self.aux_env.as_dct()}
        return dct


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
