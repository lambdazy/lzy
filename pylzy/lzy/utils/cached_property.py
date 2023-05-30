"""
We can drop this module after dropping python3.7 support
"""

try:
    from functools import cached_property  # type: ignore
except ImportError:
    from cached_property import cached_property  # type: ignore


__all__ = [
    'cached_property'
]
