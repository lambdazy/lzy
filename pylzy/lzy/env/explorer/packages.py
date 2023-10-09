from __future__ import annotations

from dataclasses import dataclass
from typing import FrozenSet, Dict

from lzy.env.base import Deconstructible


@dataclass(frozen=True)
class BasePackage(Deconstructible):
    name: str


@dataclass(frozen=True)
class LocalPackage(BasePackage):
    paths: FrozenSet[str]
    is_binary: bool


@dataclass(frozen=True)
class LocalDistribution(LocalPackage):
    version: str
    bad_paths: FrozenSet[str]


@dataclass(frozen=True)
class PypiDistribution(BasePackage):
    version: str
    pypi_index_url: str
    have_server_supported_tags: bool


@dataclass(frozen=True)
class BrokenModules(BasePackage):
    modules_paths: Dict[str, str]
