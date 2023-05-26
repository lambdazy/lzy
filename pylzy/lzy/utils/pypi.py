from functools import lru_cache
from typing import Optional, Set

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


def check_version_exists(package: ProjectPage, version: str) -> bool:
    versions: Set[str]
    if package.versions:
        versions = set(package.versions)
    else:
        versions = {p.version for p in package.packages if p.version}

    return version in versions


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


def check_package_version_exists(*, pypi_index_url: str, name: str, version: str) -> bool:
    client = get_pypi_client(pypi_index_url)

    try:
        project_page = client.get_project_page(name)
    # we considering pypi_index_url as valid url so all other errors (net, for example) will raise
    except NoSuchProjectError:
        return False

    return check_version_exists(project_page, version)
