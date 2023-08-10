from __future__ import annotations

from .base import BaseContainer


class NoContainer(BaseContainer):
    def deconstruct(self):
        return {}
