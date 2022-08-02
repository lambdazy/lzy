from urllib.parse import urlsplit
from pathlib import Path
from typing import Tuple


def url_from_bucket(
    scheme: str,
    bucket: str,
    key: str,
) -> str:
    path = Path(f"{scheme}:") / bucket / key
    return str(path)


def bucket_from_url(scheme: str, url: Path) -> Tuple[str, Path]:
    _parsed_scheme, _, path, _, _ = urlsplit(url)
    assert _parsed_scheme == scheme

    path = Path(path)
    if path.is_absolute():
        _, bucket, *other = path.parts
    else:
        bucket, *other = path.parts

    return bucket, Path(*other)
