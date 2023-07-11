import _hashlib
import hashlib
import os
from pathlib import Path
from typing import Union, List, BinaryIO
from zipfile import ZipFile


def zip_module(module_path: Union[str, Path], zip_fileobj: BinaryIO) -> None:
    with ZipFile(zip_fileobj.name, 'w') as z:
        _zip_module(module_path, z)

    zip_fileobj.seek(0)


def _zip_module(module_path: Union[str, Path], zip_file: ZipFile) -> None:
    module_path = Path(module_path)
    relative_to = module_path.parent

    paths: List[Path] = []
    if module_path.is_dir():
        for root, _, files in os.walk(module_path):
            paths.extend(Path(root) / filename for filename in files)
    else:
        paths.append(module_path)

    for path_at_fs in paths:
        path_to_write = path_at_fs.relative_to(relative_to)
        zip_file.write(path_at_fs, path_to_write)


# used as library function in external packages
def fileobj_hash_bytes(fileobj: BinaryIO) -> bytes:
    return _fileobj_hash(fileobj).digest()


def fileobj_hash_str(fileobj: BinaryIO) -> str:
    return _fileobj_hash(fileobj).hexdigest()


def _fileobj_hash(fileobj: BinaryIO) -> _hashlib.HASH:
    buf_size = 65_536  # 64kb

    md5 = hashlib.md5()

    while True:
        data = fileobj.read(buf_size)
        if not data:
            break
        md5.update(data)

    return md5
