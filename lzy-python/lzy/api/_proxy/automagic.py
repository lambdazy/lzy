import functools
from typing import Any, Type, Callable, TypeVar


def materialize_args_on_fall(f):
    """
    Decorates function f and returns new function which will materialize it's
    arguments on every call if the first call didn't succeed. Seems that usually
    it happens with some builtin types like int
    :param f: any function
    :return: new function with changed behaviour
    """

    def materialize_if_proxy(arg: Any) -> Any:
        return create_and_cache(type(arg), type(arg).__origin) \
            if isinstance(type(arg), Proxifier) \
            else arg

    @functools.wraps(f)
    def new(*args, **kwargs):
        # noinspection PyArgumentList
        val = f(*args, **kwargs)
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
        # TODO: probably try/except is worth too but let's wait and see
        if val is NotImplemented:
            kwargs = {name: materialize_if_proxy(arg_value)
                      for name, arg_value in kwargs.items()}
            args = tuple(materialize_if_proxy(arg) for arg in args)
            # it's ok if exception happens here because
            # there could be user exception
            val = f(*args, **kwargs)
        return val

    return new


class TrickDescriptor:
    def __init__(self, attr, callback):
        self._f = attr
        self.callback = callback

    def __get__(self, instance, owner):
        f = self._f
        if instance is not None:
            # for attributes such that its __get__ requires (or just receives)
            # instance:
            # call given callback and put result as new instance
            res = f.__get__(create_and_cache(type(instance), self.callback),
                            owner)

            if callable(res):
                # if __get__ returned callable then we have function in here
                # so instead of this function we should return a new one,
                # which will try to work as usual but if returned NotImplemented
                # it will materialize its arguments and try again
                #
                return materialize_args_on_fall(res)
            else:
                # if not callable, just return it as is
                return res
        # otherwise just call original __get__ without forcing evaluation
        return f.__get__(instance, owner)


def create_and_cache(proxy_cls, callback):
    if not hasattr(proxy_cls, '_origin'):
        proxy_cls._origin = callback()
    # noinspection PyProtectedMember
    return proxy_cls._origin


class Proxifier(type):
    def __new__(mcs, name, bases, attrs, t, origin_getter, cls_attrs):
        new_attrs = {
            k: TrickDescriptor(v, origin_getter) if hasattr(v, '__get__') else v
            for k, v in collect_attributes(t).items()
        }
        new_attrs.update(attrs)
        new_attrs.update(cls_attrs)
        new_attrs.update({'__origin': origin_getter})

        # omg, this single line allows proxy to wrap types with __slots__
        # just leave it here
        new_attrs.pop('__slots__', None)

        return super().__new__(mcs, name, bases, new_attrs)


def collect_attributes(cls):
    methods = {}
    for k in dir(cls):
        methods[k] = getattr(cls, k)
    return methods


T = TypeVar('T')


# TODO: LazyProxy type?
def proxy(origin_getter: Callable[[], T], t: Type[T],
          cls_attrs=None, obj_attrs=None) -> Any:
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

    # for type annotations (i.e. List[MyClass]) we should extract origin type
    if hasattr(t, '__origin__'):
        t = t.__origin__ # type: ignore

    # yea, creates new class everytime
    # probably all this stuff could be done with just one `type` call
    #
    # __pearl, hope it's hidden__
    class Pearl(metaclass=Proxifier, t=t, origin_getter=origin_getter,
                cls_attrs=cls_attrs):
        def __init__(self):
            super().__init__()
            for k, v in obj_attrs.items():
                setattr(self, k, v)

        def __getattribute__(self, item):
            if item in obj_attrs or item in cls_attrs:
                return super(type(self), self).__getattribute__(item)

            return getattr(create_and_cache(type(self), origin_getter), item)

        def __setattr__(self, item, value):
            # if trying to set attributes from obj_attrs to pearl obj
            # try to directly put stuff into __dict__ of pearl obj
            if item in obj_attrs or item in cls_attrs:
                return super(type(self), self).__setattr__(item, value)

            # if trying to set smth unspecified then just redirect
            # setattr call to underlying original object
            return setattr(create_and_cache(type(self), origin_getter), item,
                           value)

    return Pearl()  # type: ignore
