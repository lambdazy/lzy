import hashlib
import os
import sys
from io import BytesIO
from itertools import chain
from pathlib import Path
from typing import Optional
from zipfile import ZipFile
import site


def zip_module(path: str, zipfile: ZipFile):
    relative_to: Optional[str] = None
    module_parent_dir = str(Path(path).parent)
    for relative in sys.path:
        if relative == module_parent_dir:
            relative_to = relative
            break

    if relative_to is None:
        raise ValueError(f'Unexpected local module location: {path}')

    for root, dirs, files in os.walk(path):
        for file in files:
            file_path = (Path(root) / file).relative_to(relative_to)
            zipfile.write((Path(root) / file), file_path)


def fileobj_hash(fileobj: BytesIO) -> str:
    buf_size = 65_536  # 64kb

    md5 = hashlib.md5()

    while True:
        data = fileobj.read(buf_size)
        if not data:
            break
        md5.update(data)
    return md5.hexdigest()
