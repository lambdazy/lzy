# noinspection PyUnresolvedReferences
from typing import cast

from modules_for_tests.level1.level2.level3.level3 import Level3  # type: ignore


class Level2:
    @staticmethod
    def echo() -> str:
        return cast(str, Level3.echo())
