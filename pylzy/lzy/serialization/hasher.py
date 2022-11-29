import abc
import hashlib
from typing import Any
from serialzy.api import SerializerRegistry


class Hasher(abc.ABC):
    @abc.abstractmethod
    def hash(self, data: Any) -> str:
        """
        :param data: object to hash
        :return: hash result
        """

    @abc.abstractmethod
    def can_hash(self, data: Any) -> bool:
        """
        :param data: object to hash
        :return: True if object can be hashed, False otherwise
        """


class HashableFileLikeObj:
    def __init__(self):
        self._sig = hashlib.md5()

    def write(self, data: bytes) -> None:
        self._sig.update(data)

    def hash(self) -> str:
        return self._sig.hexdigest()  # type: ignore


class HashableHasher(Hasher):
    def __init__(self):
        self._hashable = frozenset([int, float])

    def hash(self, data: Any) -> str:
        return str(hash(data))

    def can_hash(self, data: Any) -> bool:
        return type(data) in self._hashable


class SerializingHasher(Hasher):
    def __init__(self, serializer: SerializerRegistry):
        self._serializer = serializer

    def hash(self, data: Any) -> str:
        handle = HashableFileLikeObj()
        self._serializer.find_serializer_by_type(type(data)).serialize(data, handle)  # type: ignore
        return handle.hash()

    def can_hash(self, data: Any) -> bool:
        # currently, we cannot determine if object is pickleable or not
        return True


class DelegatingHasher(Hasher):
    def __init__(self, serializer: SerializerRegistry):
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
