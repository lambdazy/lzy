from test_modules.level1.level2.level2 import Level2


class Level1:
    @staticmethod
    def echo() -> str:
        return Level2.echo()
