from lzy.api import LzyEnvironmentBuilder, LzyUtils, op


class A:
    def __init__(self, a: str):
        self._a = a

    def a(self) -> str:
        return self._a


class B(A):
    def __init__(self, a: str, b: str):
        super().__init__(a)
        self._b = b

    def b(self) -> str:
        return self._b


@op
def a() -> A:
    return A('a')


@op
def b(a: A) -> B:
    return B(a.a(), 'b')


def main():
    leb = LzyEnvironmentBuilder()
    env = leb.build()
    with env:
        a_res = a()
        b_res = b(a_res)
        print(b_res.a())
        print(b_res.b())


if __name__ == "__main__":
    main()
