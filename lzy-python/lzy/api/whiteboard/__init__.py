import dataclasses


def whiteboard(cls=None, namespace='', tags=[]):
    def wrap(cls):
        return whiteboard_(cls, namespace, tags)

    if cls is None:
        return wrap
    return wrap(cls)


def whiteboard_(cls, namespace, tags):
    if namespace is None:
        raise TypeError('namespace attribute must be specified')
    if not isinstance(tags, list) or not all(isinstance(elem, str) for elem in tags):
        raise TypeError('tags attribute is required to be a list of strings')
    if not isinstance(namespace, str):
        raise TypeError('namespace attribute is required to be a string')
    cls.NAMESPACE = namespace
    cls.TAGS = tags
    return cls


def is_whiteboard(obj):
    cls = obj if isinstance(obj, type) else type(obj)
    return hasattr(cls, 'NAMESPACE') and hasattr(cls, 'TAGS') and dataclasses.is_dataclass(cls)


def view(func):
    func.VIEW_DECORATOR = 'view_deco'
    return func
