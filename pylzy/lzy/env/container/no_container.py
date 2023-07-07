from __future__ import annotations

from typing import ClassVar

from .base import BaseContainer, ContainerTypes


class NoContainer(BaseContainer):
    container_type: ClassVar[ContainerTypes] = ContainerTypes.NoContainer
