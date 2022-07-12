import lzy
from lzy.api.v1.utils import File
from lzy.api.v1 import LzyRemoteEnv, op


@op
def a(fi: File) -> File:
    with fi.open("a") as fl:
        fl.write("buzz")
    return fi


if __name__ == "__main__":
    with LzyRemoteEnv().workflow(name="wf"):
        with open("/tmp/a.txt", "w") as f:
            f.write("fizz")
        file = File("/tmp/a.txt")
        ret = a(file)
        with ret.open("r") as f:
            for line in f.readlines():
                print(line)
