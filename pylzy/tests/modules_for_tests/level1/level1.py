from modules_for_tests.level1.level2.level2 import Level2  # type: ignore
from typing_extensions import get_origin


class Level1:
    @staticmethod
    def echo() -> str:
        return Level2.echo()  # type: ignore
