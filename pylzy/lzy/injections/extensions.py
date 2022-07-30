from typing import Tuple


def extend(classes: Tuple[type, ...], original_prefix: str = "original_"):
    def decorator(func):
        for cls in classes:
            original = getattr(cls, func.__name__)
            setattr(cls, func.__name__, func)
            setattr(cls, original_prefix + func.__name__, original)
        return func

    return decorator
