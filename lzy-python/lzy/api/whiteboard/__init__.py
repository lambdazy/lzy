import dataclasses
from typing import List


def whiteboard(tags: List[str], namespace=None):
    def wrap(cls):
        return whiteboard_(cls, namespace, tags)
    return wrap


def whiteboard_(cls, namespace, tags):
    if not isinstance(tags, list) or not all(isinstance(elem, str) for elem in tags):
        raise TypeError('tags attribute is required to be a list of strings')
    if not tags:
        raise TypeError('tags attribute must be specified')
    if namespace is not None and not isinstance(namespace, str):
        raise TypeError('namespace attribute is required to be a string')
    cls.LZY_WB_NAMESPACE = namespace
    cls.LZY_WB_TAGS = tags
    return cls


def is_whiteboard(obj) -> bool:
    if obj is None:
        return False

    cls = obj if isinstance(obj, type) else type(obj)
    return hasattr(cls, 'LZY_WB_NAMESPACE') and hasattr(cls, 'LZY_WB_TAGS') and dataclasses.is_dataclass(cls)


def view(func):
    func.LZY_WB_VIEW_DECORATOR = 'view_deco'
    return func
