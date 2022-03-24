import abc
from typing import Any, Iterable, Hashable

import cloudpickle


class Hasher(abc.ABC):

    @abc.abstractmethod
    def hash(self, data: Any) -> str:
        pass

    @abc.abstractmethod
    def can_hash(self, data: Any) -> bool:
        pass


class HashableHasher(Hasher):

    def hash(self, data: Any) -> str:
        return str(hash(data))

    def can_hash(self, data: Any) -> bool:
        return isinstance(data, Hashable)


class PickleHasher(Hasher):
    def hash(self, data: Any) -> str:
        return str(hash(cloudpickle.dumps(data)))

    def can_hash(self, data: Any) -> bool:
        return True


_hashers: Iterable[Hasher] = [HashableHasher(), PickleHasher()]


def hash_data(data: Any) -> str:
    for hasher in _hashers:
        if hasher.can_hash(data):
            return hasher.hash(data)
    raise TypeError("Cannot hash this data")


