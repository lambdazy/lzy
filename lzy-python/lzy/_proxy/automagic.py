import functools
from typing import Any, Type, Callable, TypeVar
from itertools import chain

DEBUG = False


def always_materialize_args_before_call(func):
    def materialize(arg: Any) -> Any:
        # pylint: disable=protected-access
        return create_and_cache(type(arg), type(arg)._constructor)

    @functools.wraps(func)
    def new(*args, **kwargs):
        is_arg_lazy_set = {
            is_proxy(arg)
            for arg in chain(args, kwargs.values())
        }

        if any(is_arg_lazy_set):
            args = tuple(
                arg if not is_proxy(arg) else materialize(arg)
                for arg in args
            )
            kwargs = {
                name: arg if not is_proxy(arg) else materialize(arg)
                for name, arg in kwargs.items()
            }
        return func(*args, **kwargs)

    return new


def materialize_args_on_fall(func):
    """
    Decorates function f and returns new function which will materialize it's
    arguments on every call if the first call didn't succeed and returned
    NotImplemented.

    Seems that usually it happens with some binary operations
    of builtin types like int -- they check type somehow differently then
    through usual isinstance call and return NotImplemented for objects of
    proxied classes.

    :param func: any function
    :return: new function with changed behaviour
    """

    def materialize_if_proxy(arg: Any) -> Any:
        return (
            # pylint: disable=protected-access
            create_and_cache(type(arg), type(arg)._constructor)
            if isinstance(type(arg), Proxifier) else
            arg
        )

    @functools.wraps(func)
    def new(*args, **kwargs):
        # noinspection PyArgumentList
        val = func(*args, **kwargs)
        # https://docs.python.org/3/library/constants.html#NotImplemented
        # in short: all binary operations return NotImplemented in case
        # if there is error related to type
        #
        # >>> class A:
        # ...     pass
        # >>> a = (10).__add__(A()) # exception is not raised here
        # >>> a
        # NotImplemented
        #
        # and `(10).__add__` is func here
        #
        # TODO: probably try/except is worth too but let's wait and see
        if val is NotImplemented:
            kwargs = {
                name: materialize_if_proxy(arg_value)
                for name, arg_value in kwargs.items()
            }
            args = tuple(materialize_if_proxy(arg) for arg in args)
            # it's ok if exception happens here because
            # there could be user exception
            val = func(*args, **kwargs)
        return val

    return new


class TrickDescriptor:
    def __init__(self, attr, callback):
        self.original_descriptor = attr
        self.callback = callback

    def __get__(self, instance, owner):
        func = self.original_descriptor
        # if called in not object context
        if instance is None:
            return func.__get__(instance, owner)

        # for attributes such that its __get__ requires (or just receives)
        # instance:
        # call given callback and put result as new instance
        res = func.__get__(create_and_cache(type(instance), self.callback), owner)

        if not callable(res):
            # if usual value just return it as is
            return res

        # but if __get__ returned callable then we have function in here
        # so instead of this function we should return a new one,
        # which will try to work as usual but if returned NotImplemented
        # it will materialize its arguments before call
        return always_materialize_args_before_call(res)


class TrickDescriptorOptional:
    def __init__(self, constructor, name):
        self.constructor = constructor
        self._name = name

    def __get__(self, instance, owner):
        if instance is None:
            proxy_cls = type(instance)
        else:
            proxy_cls = owner

        materialized_object = create_and_cache(proxy_cls, self.constructor)
        res = getattr(materialized_object, self._name)

        if hasattr(res, "__get__"):
            res = res.__get__(materialized_object, type(materialized_object))

        if not callable(res):
            return res

        return always_materialize_args_before_call(res)


def create_and_cache(proxy_cls, callback):
    if not hasattr(proxy_cls, "_origin") and not hasattr(proxy_cls, "_exception"):
        try:
            proxy_cls._origin = callback()  # pylint: disable=protected-access
        except Exception as e:
            proxy_cls._exception = e
            raise e
    # noinspection PyProtectedMember
    if hasattr(proxy_cls, "_exception"):
        raise proxy_cls._exception  # pylint: disable=protected-access
    return proxy_cls._origin  # pylint: disable=protected-access


class Proxifier(type):
    # Yeah, too many arguments in this method, but what can you do else
    # with all needed arguments of metaclass __new__?
    def __new__(
            cls, name, bases, attrs, proto_type, constructor, cls_attrs
    ):  # pylint: disable=too-many-arguments
        new_attrs = {
            k: TrickDescriptor(v, constructor) if hasattr(v, "__get__") else v
            for k, v in collect_attributes(proto_type).items()
        }
        # probably we can store data for this Proxy somewhere else
        # weakref.WeakKeyDictionary is a good candidate I think
        new_attrs.update(attrs)
        new_attrs.update(cls_attrs)
        new_attrs.update({"_constructor": constructor})

        # omg, this single line allows proxy to wrap types with __slots__
        # just leave it here
        new_attrs.pop("__slots__", None)

        return super().__new__(cls, name, bases, new_attrs)


class ProxifierOptional(type):
    def __new__(
            cls, name, bases, attrs, proto_type, constructor, cls_attrs
    ):
        new_attrs = {}
        for k in dir(proto_type):
            new_attrs[k] = TrickDescriptorOptional(constructor, k)
        for k in dir(type(None)):
            new_attrs[k] = TrickDescriptorOptional(constructor, k)
        new_attrs.update(attrs)
        new_attrs.update(cls_attrs)
        new_attrs.update({"_constructor": constructor})

        new_attrs.pop("__slots__", None)

        return super().__new__(cls, name, bases, new_attrs)


def collect_attributes(cls):
    methods = {}
    for k in dir(cls):
        methods[k] = getattr(cls, k)
    return methods


T = TypeVar("T")  # pylint: disable=invalid-name


def is_proxy(obj: Any) -> bool:
    return isinstance(type(obj), Proxifier) or isinstance(type(obj), ProxifierOptional)


# TODO: LazyProxy type?
def proxy(
        constructor: Callable[[], T], proto_type: Type[T], cls_attrs=None, obj_attrs=None
) -> Any:
    """
    Function which returns proxy on object, i.e. object which looks like original,
    but lazy and created by calling given callback at the last moment
    >>> a = proxy(lambda: 3, int)
    >>> for i in range(a):
    >>>     print(i)
    >>> 0
    >>> 1
    >>> 2
    """
    cls_attrs = cls_attrs or {}
    obj_attrs = obj_attrs or {}

    # yea, creates new class everytime
    # probably all this stuff could be done with just one `type` call
    #
    # __pearl, hope it's hidden__
    class Pearl(
        metaclass=Proxifier,
        proto_type=proto_type,
        constructor=constructor,
        cls_attrs=cls_attrs,
    ):
        def __init__(self):
            if DEBUG:
                print("Pearl __init__ call")

            super().__init__()
            for name, attr in obj_attrs.items():
                setattr(self, name, attr)

        # for cases when user-defined class has custom __new__
        def __new__(cls):
            return super().__new__(cls)

        def __getattribute__(self, item):
            if DEBUG:
                print(f"Called __getattribute__: {item}")

            if item == '__lzy_origin__':
                create_and_cache(type(self), constructor)
                # noinspection PyProtectedMember
                return type(self)._origin  # pylint: disable=protected-access
            elif item == '__lzy_materialized__':
                return hasattr(type(self), '_origin')

            if item in obj_attrs or item in cls_attrs:
                candidate = super().__getattribute__(item)
            else:
                candidate = getattr(create_and_cache(type(self), constructor), item)

            if DEBUG:
                print(f"and gonna return {candidate}")

            return candidate

        def __setattr__(self, item, value):
            # if trying to set attributes from obj_attrs to pearl obj
            # try to directly put stuff into __dict__ of pearl obj
            if item in obj_attrs or item in cls_attrs:
                return super().__setattr__(item, value)

            # if trying to set smth unspecified then just redirect
            # setattr call to underlying original object
            return setattr(create_and_cache(type(self), constructor), item, value)

    return Pearl()  # type: ignore


def proxy_optional(
        constructor: Callable[[], T], proto_type: Type[T], cls_attrs=None, obj_attrs=None
) -> Any:
    cls_attrs = cls_attrs or {}
    obj_attrs = obj_attrs or {}

    class Pearl(
        metaclass=ProxifierOptional,
        proto_type=proto_type,
        constructor=constructor,
        cls_attrs=cls_attrs,
    ):
        def __init__(self):
            if DEBUG:
                print("Pearl __init__ call")

            super().__init__()
            for name, attr in obj_attrs.items():
                setattr(self, name, attr)

        # for cases when user-defined class has custom __new__
        def __new__(cls):
            return super().__new__(cls)

        def __getattribute__(self, item):
            if DEBUG:
                print(f"Called __getattribute__: {item}")

            if item in obj_attrs or item in cls_attrs:
                candidate = super().__getattribute__(item)
            else:
                candidate = getattr(create_and_cache(type(self), constructor), item)

            if DEBUG:
                print(f"and gonna return {candidate}")

            return candidate

        def __setattr__(self, item, value):
            # if trying to set attributes from obj_attrs to pearl obj
            # try to directly put stuff into __dict__ of pearl obj
            if item in obj_attrs or item in cls_attrs:
                return super().__setattr__(item, value)

            # if trying to set smth unspecified then just redirect
            # setattr call to underlying original object
            return setattr(create_and_cache(type(self), constructor), item, value)

    return Pearl()  # type: ignore
