from tests.test_modules.level1.level2.level3.level3 import Level3
# noinspection PyUnresolvedReferences
import s3fs


class Level2:
    @staticmethod
    def echo() -> str:
        return Level3.echo()
