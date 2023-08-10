from __future__ import annotations

from typing import TYPE_CHECKING

from lzy.env.base import Deconstructible

if TYPE_CHECKING:
    from lzy.env.mixin import WithEnvironmentType


class BaseContainer(Deconstructible):
    def __call__(self, subject: WithEnvironmentType) -> WithEnvironmentType:
        return subject.with_container(self)
