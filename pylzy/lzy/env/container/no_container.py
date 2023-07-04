from __future__ import annotations

from typing import ClassVar

from .base import BaseContainer, SupportedContainerTypes


class NoContainer(BaseContainer):
    container_type: ClassVar[SupportedContainerTypes] = SupportedContainerTypes.NoContainer
