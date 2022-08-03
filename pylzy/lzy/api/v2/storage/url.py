import os
from urllib.parse import urlsplit, urljoin
from pathlib import Path
from typing import Tuple

from enum import Enum


class Scheme(Enum):
    s3 = 1
    azure = 2


def url_from_bucket(
    scheme: Scheme,
    bucket: str,
    key: str,
) -> str:
    path = Path(f"{scheme.name}:") / bucket / key
    return str(path)


def bucket_from_url(scheme: Scheme, url: Path) -> Tuple[str, Path]:
    _parsed_scheme, _, _path, _, _ = urlsplit(str(url))
    assert _parsed_scheme == scheme.name

    path = Path(_path)
    if path.is_absolute():
        _, bucket, *other = path.parts
    else:
        bucket, *other = path.parts

    return bucket, Path(*other)
