from pathlib import Path, PurePath
from typing import Union, Tuple
from appdirs import user_cache_dir

APPNAME = 'lzy'


def get_cache_path(*parts: Union[str, PurePath]) -> Path:
    base = user_cache_dir(APPNAME)
    path = Path(base).joinpath(*parts)

    directory = path.parent
    directory.mkdir(parents=True, exist_ok=True)

    return path
