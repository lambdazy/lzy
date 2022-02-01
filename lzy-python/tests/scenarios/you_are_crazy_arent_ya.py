from typing import Callable
from lzy.api import op, LzyLocalEnv  # pylint: disable=no-name-in-module

SOME_GLOBAL: int = 0


# pylint: disable=invalid-name
@op
def no() -> None:
    # pylint: disable=global-statement
    global SOME_GLOBAL
    SOME_GLOBAL = 50


@op
def noo() -> int:
    return SOME_GLOBAL


NotType = Callable[[], int]
SurelytType = Callable[[], NotType]


@op
def im() -> SurelytType:
    first = 89

    def surely() -> NotType:
        second = 144

        def no_t() -> int:
            nonlocal first
            nonlocal second
            first, second = second, first + second
            return second

        return no_t

    return surely


@op
def heh(surely: SurelytType) -> int:
    return surely()()


if __name__ == "__main__":
    with LzyLocalEnv():
        value: int = heh(im())
        # or
        nothing: None = no()
        something: int = noo()
        print(value + 1)
        print(type(nothing), nothing)
