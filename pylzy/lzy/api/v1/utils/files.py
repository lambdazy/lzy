import hashlib
import os
from io import BytesIO
from pathlib import Path
from typing import Union, List
from zipfile import ZipFile


def zip_module(path: Union[str, Path], zipfile: ZipFile):
    path = Path(path)
    relative_to = path.parent

    paths: List[Path] = []
    if path.is_dir():
        for root, _, files in os.walk(path):
            paths.extend(Path(root) / filename for filename in files)
    else:
        paths.append(path)

    for path_at_fs in paths:
        path_to_write = path_at_fs.relative_to(relative_to)
        zipfile.write(path_at_fs, path_to_write)


def fileobj_hash(fileobj: BytesIO) -> str:
    buf_size = 65_536  # 64kb

    md5 = hashlib.md5()

    while True:
        data = fileobj.read(buf_size)
        if not data:
            break
        md5.update(data)
    return md5.hexdigest()
