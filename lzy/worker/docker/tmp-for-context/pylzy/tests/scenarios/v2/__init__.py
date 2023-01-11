from lzy.api.v1 import op, Lzy


@op
def a(s: int) -> int:
    print(s)
    return s


@op
def b(s1: int, s2: int) -> str:
    print(s1, s2)
    return str(s1 + s2)


def run():
    lzy = Lzy()
    lzy.auth(user="lzy-internal-user", key_path="/tmp/key", endpoint="localhost:13579")
    with lzy.workflow("test", interactive=False):
        s1 = a(21)
        s2 = a(21)
        ret = b(s1, s2)
        print(ret)


if __name__ == "__main__":
    run()
