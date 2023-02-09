import functools
from itertools import chain
from typing import Any, Callable, Dict, Optional, Type, Tuple, Sequence

DEBUG = False


def always_materialize_args_before_call(func):
    def materialize(arg: Any) -> Any:
        # noinspection PyProtectedMember
        # pylint: disable=protected-access
        return create_and_cache(type(arg), type(arg)._constructor)

    @functools.wraps(func)
    def new(*args, **kwargs):
        is_arg_lazy_set = {is_proxy(arg) for arg in chain(args, kwargs.values())}

        if any(is_arg_lazy_set):
            args = tuple(arg if not is_proxy(arg) else materialize(arg) for arg in args)
            kwargs = {
                name: arg if not is_proxy(arg) else materialize(arg)
                for name, arg in kwargs.items()
            }
        return func(*args, **kwargs)

    return new


class TrickDescriptor:
    def __init__(self, name: str, constructor: Callable[[], Any]):
        self.__name = name
        self.__constructor = constructor

    def __get__(self, instance: Any, owner: Type):
        # if called in not object context
        if instance is None:
            proxy_cls = type(instance)
        else:
            proxy_cls = owner

        materialized_object = create_and_cache(proxy_cls, self.__constructor)
        # handle __bool__() func
        if self.__name == '__bool__' and not hasattr(materialized_object, self.__name):
            if materialized_object:
                return lambda: True
            else:
                return lambda: False

        res = getattr(materialized_object, self.__name)
        # for attributes such that its __get__ requires (or just receives)
        # instance:
        # call given callback and put result as new instance
        if hasattr(res, "__get__"):
            res = res.__get__(materialized_object, type(materialized_object))

        if not callable(res):
            # if usual value just return it as is
            return res

        # but if __get__ returned callable then we have function in here
        # so instead of this function we should return a new one,
        # which will **always** materialize its arguments before call
        return always_materialize_args_before_call(res)


def create_and_cache(proxy_cls: Type, callback: Callable[[], Any]):
    if not hasattr(proxy_cls, "_origin") and not hasattr(proxy_cls, "_exception"):  # type: ignore[attr-defined]
        try:
            proxy_cls._origin = (
                callback()
            )  # pylint: disable=protected-access # type: ignore[attr-defined]
        except Exception as e:
            proxy_cls._exception = e
            raise e
    if hasattr(proxy_cls, "_exception"):
        # noinspection PyProtectedMember
        raise proxy_cls._exception  # pylint: disable=protected-access
    # noinspection PyProtectedMember
    return (
        proxy_cls._origin
    )  # pylint: disable=protected-access # type: ignore[attr-defined]


class Proxifier(type):
    def __new__(mcs, name: str, bases: Tuple[Type], attrs: Dict[str, Any], proto_types: Sequence[Type],
                constructor: Callable[[], Any], cls_attrs: Dict[str, Any]):
        new_attrs: Dict[str, Any] = {}
        for typ in proto_types:
            for prop in dir(typ):
                new_attrs[prop] = TrickDescriptor(prop, constructor)

        # probably we can store data for this Proxy somewhere else
        # weakref.WeakKeyDictionary is a good candidate I think
        new_attrs.update(attrs)
        new_attrs.update(cls_attrs)
        new_attrs.update({"_constructor": constructor})

        # this single line allows proxy to wrap types with __slots__
        # just leave it here
        new_attrs.pop("__slots__", None)

        return super().__new__(mcs, name, bases, new_attrs)


def is_proxy(obj: Any) -> bool:
    return isinstance(type(obj), Proxifier) or isinstance(type(obj), Proxifier)


def proxy(
    constructor: Callable[[], Any],
    proto_types: Sequence[Type],
    cls_attrs: Optional[Dict[str, Any]] = None,
    obj_attrs: Optional[Dict[str, Any]] = None,
) -> Any:
    """
    Function which returns proxy on object, i.e. object which looks like original,
    but lazy and created by calling given callback at the last moment
    >>> a = proxy(lambda: 3, (int,))
    >>> for i in range(a):
    >>>     print(i)
    >>> 0
    >>> 1
    >>> 2
    """
    _cls_attrs: Dict[str, Any] = cls_attrs or {}
    _obj_attrs: Dict[str, Any] = obj_attrs or {}

    # yea, creates new class everytime
    # probably all this stuff could be done with just one `type` call
    #
    # __pearl, hope it's hidden__
    class Pearl(
        metaclass=Proxifier,
        proto_types=proto_types,
        constructor=constructor,
        cls_attrs=_cls_attrs,
    ):
        def __init__(self):
            if DEBUG:
                print("Pearl __init__ call")

            super().__init__()
            for name, attr in _obj_attrs.items():
                setattr(self, name, attr)

        # for cases when user-defined class has custom __new__
        def __new__(cls):
            return super().__new__(cls)

        def __getattribute__(self, item):
            if DEBUG:
                print(f"Called __getattribute__: {item}")

            if item == "__lzy_origin__":
                create_and_cache(type(self), constructor)
                # noinspection PyProtectedMember
                # pylint: disable=protected-access
                return type(self)._origin  # type: ignore[attr-defined]
            elif item == "__lzy_materialized__":
                return hasattr(type(self), "_origin")

            if item in _obj_attrs or item in _cls_attrs:
                candidate = super().__getattribute__(item)
            else:
                candidate = getattr(
                    create_and_cache(type(self), constructor),
                    item,
                )

            if DEBUG:
                print(f"and gonna return {candidate}")

            return candidate

        def __setattr__(self, item, value):
            # if trying to set attributes from obj_attrs to pearl obj
            # try to directly put stuff into __dict__ of pearl obj
            if item in _obj_attrs or item in _cls_attrs:
                return super().__setattr__(item, value)

            # if trying to set smth unspecified then just redirect
            # setattr call to underlying original object
            return setattr(
                create_and_cache(type(self), constructor),
                item,
                value,
            )

    return Pearl()  # type: ignore
