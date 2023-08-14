import os
from contextlib import contextmanager
from pathlib import Path, PurePath
from typing import Union
from appdirs import user_cache_dir

APPNAME = 'lzy'


def get_cache_path(*parts: Union[str, PurePath]) -> Path:
    base = user_cache_dir(APPNAME)
    path = Path(base).joinpath(*parts)

    directory = path.parent
    directory.mkdir(parents=True, exist_ok=True)

    return path


@contextmanager
def change_working_directory(path: str):
    old_cwd = os.getcwd()

    os.chdir(path)

    try:
        yield
    finally:
        os.chdir(old_cwd)
