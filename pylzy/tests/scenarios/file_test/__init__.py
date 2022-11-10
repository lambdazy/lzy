import dataclasses

from lzy.whiteboards.whiteboard import WhiteboardRepository

from lzy.api.v2 import op, whiteboard, Lzy
from lzy.api.v2.remote_grpc.runtime import GrpcRuntime
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
@whiteboard(name="wb")
class FileWb:
    f2: File
    f3: File


if __name__ == "__main__":
    wb_id = ""

    runtime = GrpcRuntime()
    lzy = Lzy(runtime=runtime)

    with lzy.workflow("wf", interactive=False) as wf:
        file_wb = wf.create_whiteboard(FileWb)

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
        wb_id = file_wb.whiteboard_id

        i, s = b()
        print(f"{i}, {s}")

    wb_repo = WhiteboardRepository(lzy.storage_registry, lzy.serializer)
    wb = wb_repo.get(wb_id)
    with wb.f2.open("r") as f:
        for line in f.readlines():
            print(f"wb: {line}")
    with wb.f3.open("r") as f:
        for line in f.readlines():
            print(f"wb: {line}")
