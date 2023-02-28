import hashlib
from functools import wraps
from typing import cast, IO


def md5_of_str(uri: str) -> str:
    return hashlib.md5(uri.encode('utf-8')).hexdigest()


class HashingIO:
    def __init__(self, io_stream: IO[bytes]):
        self.io_stream: IO[bytes] = io_stream
        self.__md5 = hashlib.md5()

    @property
    def md5(self) -> str:
        return cast(str, self.__md5.hexdigest())

    def __getattr__(self, name: str):
        io_stream = self.__dict__['io_stream']
        attr = getattr(io_stream, name)

        if hasattr(attr, '__call__'):
            func = attr

            @wraps(func)
            def simple_wrapper(*args, **kwargs):
                return func(*args, **kwargs)
            attr = simple_wrapper

        if not isinstance(attr, int):
            setattr(self, name, attr)

        return attr

    def write(self, s: bytes) -> int:
        self.__md5.update(s)
        return self.io_stream.write(s)

    def __enter__(self):
        self.io_stream.__enter__()
        return self

    def __exit__(self, exc, value, tb):
        result = self.io_stream.__exit__(exc, value, tb)
        self.close()
        return result

    def __iter__(self):
        for line in self.io_stream:
            yield line
