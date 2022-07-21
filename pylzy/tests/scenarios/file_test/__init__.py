import dataclasses
from typing import Optional

from lzy.api.v1 import LzyRemoteEnv, op
from lzy.api.v1.whiteboard import whiteboard
from lzy.serialization.types import File


@op
def a(f1: File) -> (File, File, File):
    with f1.open("a") as fl:
        fl.write("buzz")

    with open("/tmp/b.txt", "w") as fl:
        fl.write("Lol")

    with open("/tmp/c.txt", "w") as fl:
        fl.write("Kek")

    return f1, File("/tmp/b.txt"), File("/tmp/c.txt")


@op
def b() -> (int, str):
    return 42, "42"


@dataclasses.dataclass
@whiteboard(tags=["tag"])
class FileWb:
    f2: Optional[File] = None
    f3: Optional[File] = None


if __name__ == "__main__":
    file_wb = FileWb()
    wb_id = ""
    with LzyRemoteEnv().workflow(name="wf", whiteboard=file_wb) as wf:
        with open("/tmp/a.txt", "w") as f:
            f.write("fizz")
        file = File("/tmp/a.txt")
        f1, f2, f3 = a(file)
        with f1.open("r") as f:
            for line in f.readlines():
                print(line)
        with f2.open("r") as f:
            for line in f.readlines():
                print(line)
        with f3.open("r") as f:
            for line in f.readlines():
                print(line)

        file_wb.f2 = f2
        file_wb.f3 = f3
        wb_id = file_wb.__id__

        i, s = b()
        print(f"{i}, {s}")

    env = LzyRemoteEnv()
    wb = env.whiteboard_by_id(wb_id)
    with wb.f2.open("r") as f:
        for line in f.readlines():
            print(f"wb: {line}")
    with wb.f3.open("r") as f:
        for line in f.readlines():
            print(f"wb: {line}")
