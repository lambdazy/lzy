import abc
import hashlib
from typing import Any

from lzy.serialization.serializer import FileSerializer


class HashableFileLikeObj:
    def __init__(self):
        self._sig = hashlib.md5()

    def write(self, data: bytes) -> None:
        self._sig.update(data)

    def hash(self) -> str:
        return self._sig.hexdigest() # type: ignore


class Hasher(abc.ABC):
    @abc.abstractmethod
    def hash(self, data: Any) -> str:
        pass

    @abc.abstractmethod
    def can_hash(self, data: Any) -> bool:
        pass


class HashableHasher(Hasher):
    def __init__(self):
        self._hashable = frozenset([int, float])

    def hash(self, data: Any) -> str:
        return str(hash(data))

    def can_hash(self, data: Any) -> bool:
        return type(data) in self._hashable


class SerializingHasher(Hasher):
    def __init__(self, serializer: FileSerializer):
        self._serializer = serializer

    def hash(self, data: Any) -> str:
        handle = HashableFileLikeObj()
        self._serializer.serialize_to_file(data, handle)  # type: ignore
        return handle.hash()

    def can_hash(self, data: Any) -> bool:
        # currently, we cannot determine if object is pickleable or not
        return True


class DelegatingHasher(Hasher):
    def __init__(self, serializer: FileSerializer):
        self._hashers = [HashableHasher(), SerializingHasher(serializer)]

    def hash(self, data: Any) -> str:
        for hasher in self._hashers:
            if hasher.can_hash(data):
                return hasher.hash(data)
        raise ValueError("Cannot hash data of type: " + type(data))

    def can_hash(self, data: Any) -> bool:
        for hasher in self._hashers:
            if hasher.can_hash(data):
                return True
        return False
