from enum import Enum
from pathlib import Path
from typing import Tuple, Iterable
from urllib.parse import urlsplit


class Scheme(Enum):
    s3 = 1
    azure = 2


def uri_from_bucket(
    scheme: Scheme,
    bucket: str,
    key: str,
) -> str:
    path = f"{scheme.name}://{bucket}/{key}"
    return path


def bucket_from_uri(scheme: Scheme, uri: str) -> Tuple[str, str]:
    parsed_scheme, bucket, _path, _, _ = urlsplit(uri)
    assert parsed_scheme == scheme.name

    path = Path(_path)
    other: Iterable[str]
    if path.is_absolute():
        _, *other = path.parts
    else:
        other = path.parts

    return bucket, str(Path(*other))
