from enum import Enum
from pathlib import Path
from typing import Tuple
from urllib.parse import urlsplit


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


def bucket_from_url(scheme: Scheme, url: str) -> Tuple[str, str]:
    _parsed_scheme, _, _path, _, _ = urlsplit(url)
    assert _parsed_scheme == scheme.name

    path = Path(_path)
    if path.is_absolute():
        _, bucket, *other = path.parts
    else:
        bucket, *other = path.parts

    return bucket, str(Path(*other))
