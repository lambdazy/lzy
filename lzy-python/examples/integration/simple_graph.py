from lzy.api import op, LzyRemoteEnv
import base
from base import Base
import cloudpickle
from lzy.servant.terminal_server import TerminalConfig


def main():
    @op
    def str_gen() -> str:
        return "More meaningful str than ever "

    @op
    def foo(a: int) -> Base:
        return Base(a, "before")

    @op
    def bar(bs: Base, sp: str) -> str:
        # noinspection PyTypeChecker
        return sp + bs.b + str(bs.a)
    with LzyRemoteEnv():
        s = str_gen()
        f = foo(3)
        b = bar(f, s)

    print(b)


if __name__ == '__main__':
    cloudpickle.register_pickle_by_value(base)
    main()
