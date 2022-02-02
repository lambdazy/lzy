from base import Base
from lzy.api import op, LzyRemoteEnv


def main():
    @op
    def str_gen() -> str:
        return "More meaningful str than ever "

    @op
    def foo(a: int) -> Base:
        base = Base(a, "before")
        print(base.echo())
        return base

    @op
    def just_print() -> None:
        print("Just print some text")

    @op
    def bar(bs: Base, sp: str) -> str:
        # noinspection PyTypeChecker
        return sp + bs.b + str(bs.a)

    with LzyRemoteEnv():
        just_print()
        s = str_gen()
        f = foo(3)
        b = bar(f, s)

    print(b)


if __name__ == '__main__':
    main()
