"""
We can drop this module after dropping python3.7 support
"""

try:
    from functools import cached_property
except ImportError:
    from cached_property import cached_property


__all__ = [
    'cached_property'
]
