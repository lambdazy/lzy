import abc
import logging
from typing import Dict, Tuple, Iterable


class Env(abc.ABC):
    # @abc.abstractmethod
    # def name(self) -> str:
    #     pass

    # probably it's better to declare to_json method
    # but it doesn't work with google protobuf json parser
    # and return type probably is not the best choice but ok for now
    @abc.abstractmethod
    def as_dct(self) -> Dict[str, str]:
        pass

    @abc.abstractmethod
    def type_id(self) -> str:
        pass


class PyEnv(Env):
    def __init__(self, env_name: str, yaml: str):
        super().__init__()
        self._name = env_name
        self._yaml = yaml
        self._log = logging.getLogger(str(self.__class__))

    def type_id(self) -> str:
        return 'pyenv'

    def name(self) -> str:
        return self._name

    def yaml(self) -> str:
        return self._yaml

    def as_dct(self) -> Dict[str, str]:
        return {
            'name': self._name,
            'yaml': self._yaml
        }


PACKAGES_DELIM = ';'
PACKAGE_VERS_DELIM = '=='


def to_str_version(version: Tuple[str]) -> str:
    return ".".join(version)


def to_str_packages(packages: Dict[str, Tuple[str]]) -> str:
    return join_packages((n, '.'.join(v)) for n, v in packages.items())


def join_packages(packages: Iterable[Tuple[str, str]]) -> str:
    return PACKAGES_DELIM.join(
        PACKAGE_VERS_DELIM.join(pack_info) for pack_info in packages
    )


def parse_version(version: str) -> Tuple[str, ...]:
    return tuple(version.split('.'))
