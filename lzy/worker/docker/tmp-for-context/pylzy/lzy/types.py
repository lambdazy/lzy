from pathlib import Path
from typing import TextIO, cast


class File:
    def __init__(self, path: str):
        self.__path = Path(path)
        if not self.__path.exists() or not self.__path.is_file():
            raise ValueError("File path must points to file")

    @property
    def path(self) -> Path:
        return self.__path

    def copy(self) -> "File":
        return File(str(self.__path))

    def open(self, *args, **kwargs) -> TextIO:
        return cast(TextIO, self.__path.open(*args, **kwargs))
