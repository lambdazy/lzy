from typing import Dict, Iterable, Tuple

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
