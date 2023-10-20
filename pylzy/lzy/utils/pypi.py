from functools import lru_cache
from typing import Optional, Set, Tuple, FrozenSet

from packaging.tags import (
    compatible_tags,
    cpython_tags,
    PythonVersion,
    Tag,
)
from packaging.utils import parse_wheel_filename

import requests
import requests.exceptions

from pypi_simple import (
    PyPISimple,
    ProjectPage,
    PYPI_SIMPLE_ENDPOINT,
    ACCEPT_JSON_PREFERRED,
    NoSuchProjectError
)

from lzy import config
from lzy.exceptions import BadPypiIndex
from lzy.version import __user_agent__

PIP_VERSION_REQ = "10.0.0"
PYPI_INDEX_URL_DEFAULT = PYPI_SIMPLE_ENDPOINT

# TODO: obtain this information from server
TARGET_PLATFORMS: Tuple[str, ...] = (
    'linux_x86_64',
    'manylinux1_x86_64',
    'manylinux2010_x86_64',
    'manylinux2014_x86_64',
) + tuple(
    f'manylinux_2_{i}_x86_64'
    for i in range(5, 32)  # 5..31; at the server ubuntu 20.04 which have glibc 2.31
)


@lru_cache(maxsize=None)
def get_pypi_client(url: str) -> PyPISimple:
    # NB: i think we don't need to close this session, it will
    # closed with exit
    session = requests.session()
    session.headers["User-Agent"] = __user_agent__

    return PyPISimple(
        endpoint=url,
        session=session,
        accept=ACCEPT_JSON_PREFERRED
    )


@lru_cache(maxsize=None)
def get_compatible_tags(target_python: PythonVersion, target_platforms: Tuple[str, ...]) -> FrozenSet[Tag]:
    result: Set[Tag] = set()

    result.update(
        compatible_tags(python_version=target_python, platforms=target_platforms)
    )

    result.update(
        cpython_tags(python_version=target_python, platforms=target_platforms)
    )

    return frozenset(result)


def check_version_exists(
    package: ProjectPage,
    version: str,
) -> bool:
    versions: Set[str]
    if package.versions:
        versions = set(package.versions)
    else:
        versions = {p.version for p in package.packages if p.version}

    return version in versions


def check_version_exists_on_target_platform(
    package: ProjectPage,
    version: str,
    target_python: PythonVersion,
    target_platforms: Tuple[str, ...] = TARGET_PLATFORMS,
) -> bool:
    for p in package.packages:
        if p.version != version:
            continue

        if p.filename.endswith('.whl'):
            *_, tags = parse_wheel_filename(p.filename)

            if tags & get_compatible_tags(target_python, target_platforms):
                return True

        elif p.filename.endswith('.zip') or p.filename.endswith('.gz'):
            # probably it is sdist, here may be problems
            return True
        else:
            # .exe, for example
            continue

    return False


def validate_pypi_index_url(pypi_index_url: str) -> None:
    if config.skip_pypi_validation:
        return

    exception: Optional[Exception] = None
    client = get_pypi_client(pypi_index_url)

    try:
        project_page = client.get_project_page('pip')

        if check_version_exists(project_page, PIP_VERSION_REQ):
            return  # all good

    except requests.Timeout:
        raise  # this url could be good, but we experiencing net problems
    except (NoSuchProjectError, requests.exceptions.RequestException) as e:
        exception = e  # all other errors we will reraize as BadPypiIndex

    # raise from clause is ok with None value
    raise BadPypiIndex(
        f"failed to find pip=={PIP_VERSION_REQ} at pypi_index_url=={pypi_index_url}; "
        f"check if it is correct simple pypi index url, for example - {PYPI_SIMPLE_ENDPOINT}"
    ) from exception


@lru_cache(maxsize=16)  # cache will work well with consequetive requests
def get_project_page(*, pypi_index_url: str, name: str) -> Optional[ProjectPage]:
    client = get_pypi_client(pypi_index_url)

    try:
        return client.get_project_page(name)
    # we considering pypi_index_url as valid url so all other errors (net, for example) will raise
    except NoSuchProjectError:
        return None


@lru_cache(maxsize=None)
def check_package_version_exists(*, pypi_index_url: str, name: str, version: str) -> bool:
    project_page = get_project_page(
        pypi_index_url=pypi_index_url,
        name=name,
    )
    if not project_page:
        return False

    return check_version_exists(project_page, version)


@lru_cache(maxsize=None)
def check_package_version_exists_on_target_platform(
    *,
    pypi_index_url: str,
    name: str,
    version: str,
    target_python: PythonVersion,
    target_platforms: Tuple[str, ...] = TARGET_PLATFORMS,
) -> bool:
    project_page = get_project_page(
        pypi_index_url=pypi_index_url,
        name=name,
    )
    if not project_page:
        return False

    return check_version_exists_on_target_platform(
        project_page,
        version,
        target_python=target_python,
        target_platforms=target_platforms,
    )
