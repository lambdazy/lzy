import hashlib
from functools import wraps
from hashlib import _Hash


def md5_of_str(uri: str) -> str:
    return hashlib.md5(uri.encode('utf-8')).hexdigest()


class HashingFile:
    def __init__(self, file):
        self.file = file
        self.__md5: _Hash = hashlib.md5()

    @property
    def md5(self) -> str:
        return self.__md5.hexdigest()

    def __getattr__(self, name):
        file = self.__dict__['file']
        attr = getattr(file, name)

        if hasattr(attr, '__call__'):
            if name == 'write':
                func = attr

                @wraps(func)
                def wrapper(*args, **kwargs):
                    if args:
                        self.__md5.update(args[0])
                    elif kwargs:
                        self.__md5.update(next(iter(kwargs.values())))
                    return func(*args, **kwargs)

                wrapper._closer = self._closer
                attr = wrapper
            else:
                func = attr

                @wraps(func)
                def simple_wrapper(*args, **kwargs):
                    return func(*args, **kwargs)

                simple_wrapper._closer = self._closer
                attr = simple_wrapper

        if not isinstance(attr, int):
            setattr(self, name, attr)

        return attr

    def __enter__(self):
        self.file.__enter__()
        return self

    def __exit__(self, exc, value, tb):
        result = self.file.__exit__(exc, value, tb)
        self.close()
        return result

    def close(self):
        self._closer.close()

    def __iter__(self):
        for line in self.file:
            yield line
