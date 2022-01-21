from lzy.api import op, LzyRemoteEnv
from lzy.servant.terminal_server import TerminalConfig


def main():
    @op
    def function_returning_1() -> int:
        return 1

    @op
    def function_returning_2() -> int:
        return 2

    @op
    def function_returning_3() -> int:
        return 3

    @op
    def function_getting_1_2_3_asserting_them_and_summing_up(a: int, b: int, c: int) -> int:
        assert a == 1, "a is not 1!"
        assert b == 2, "b is not 2!"
        assert c == 3, "c is not 3!"
        return a + b + c

    with LzyRemoteEnv():
        a = function_returning_1()
        b = function_returning_2()
        c = function_returning_3()
        result = function_getting_1_2_3_asserting_them_and_summing_up(a, b, c)

    assert result == 6, "Result should be equal 6"
    print(result)


if __name__ == '__main__':
    main()
