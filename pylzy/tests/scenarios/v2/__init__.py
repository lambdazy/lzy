from lzy.api.v2 import op, Lzy
from lzy.api.v2.remote_grpc.runtime import GrpcRuntime


@op
def a(s: int) -> int:
    return s


@op
def b(s1: int, s2: int) -> str:
    return str(s1 + s2)


def run():

    l = Lzy(runtime=GrpcRuntime("lzy-internal-user", "localhost:13579", "/tmp/key"))

    with l.workflow("test", interactive=False):
        s1 = a(21)
        s2 = a(21)
        ret = b(s1, s2)
        print(ret)


if __name__ == "__main__":
    run()
