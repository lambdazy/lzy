from pathlib import Path
from typing import Tuple
from urllib.parse import urlparse


def bucket_from_url(url: str) -> Tuple[str, str]:
    path = Path(urlparse(url).path)
    if path.is_absolute():
        _, bucket, *other = path.parts
    else:
        bucket, *other = path.parts

    return bucket, str(Path(*other))
