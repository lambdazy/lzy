import hashlib
import os
from io import BytesIO
from pathlib import Path
from zipfile import ZipFile


def zipdir(path: str, zipfile: ZipFile):
    for root, dirs, files in os.walk(path):
        for file in files:
            file_path = (Path(root) / file).relative_to(Path.cwd())
            zipfile.write(file_path, file_path)


def fileobj_hash(fileobj: BytesIO) -> str:
    buf_size = 65_536  # 64kb

    md5 = hashlib.md5()

    while True:
        data = fileobj.read(buf_size)
        if not data:
            break
        md5.update(data)
    return md5.hexdigest()
