import inspect
from abc import abstractmethod
from typing import Type, Any

NON_OVERLOADING_MEMBERS = ['__class__', '__getattribute__', '__setattr__', '__new__', '__init__',
                           '__init_subclass__', '__abstractmethods__', '__dict__', '__weakref__']
DEFAULT_MEMBERS = ['__init__', 'call', '__module__', '__dict__', '__weakref__', '__doc__', 'cleanup']


class Proxy:
    def __init__(self, typ: Type):
        self._type = typ
        setattr(self, 'typ', lambda *a: self._type)
        members = inspect.getmembers(typ)
        for (k, v) in members:
            if k not in NON_OVERLOADING_MEMBERS:
                # noinspection PyShadowingNames
                setattr(self, k, lambda *a, k=k: self.call(k, *a))
                # noinspection PyShadowingNames
                setattr(Proxy, k, lambda obj, *a, k=k: getattr(obj, k)(*a))

    @abstractmethod
    def call(self, name: str, *args) -> Any:
        pass

    @staticmethod
    def cleanup():
        for k in Proxy.__dict__.keys():
            if k not in DEFAULT_MEMBERS:
                setattr(Proxy, k, None)
