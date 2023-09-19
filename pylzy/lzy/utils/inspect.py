from __future__ import annotations

try:
    from inspect import get_annotations  # type: ignore
except ImportError:
    from get_annotations import get_annotations  # type: ignore


__all__ = [
    'get_annotations',
]
