import dataclasses

from lzy.api.v1 import op, whiteboard, Lzy, lzy_auth
from lzy.types import File


# noinspection PyShadowingNames
@op(gpu_type="NO_GPU")
def a(f1: File) -> (File, File, File):
    with f1.open("a") as fl:
        fl.write("buzz")

    with open("/tmp/b.txt", "w") as fl:
        fl.write("Lol")

    with open("/tmp/c.txt", "w") as fl:
        fl.write("Kek")

    return f1, File("/tmp/b.txt"), File("/tmp/c.txt")


@op(gpu_type="NO_GPU")
def b() -> (int, str):
    return 42, "42"


@dataclasses.dataclass
@whiteboard(name="wb")
class FileWb:
    f2: File
    f3: File


if __name__ == "__main__":
    wb_id = ""
    lzy = Lzy()

    lzy_auth(
        user="ArtoLord",
        key_path="/Users/artolord/.ssh/private.pem",
        endpoint="158.160.44.118:8122",
        whiteboards_endpoint="158.160.34.24:8122"
    )

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
        wb_id = file_wb.id

        i, s = b()
        print(f"{i}, {s}")

    wb = lzy.whiteboard(id_=wb_id)
    with wb.f2.open("r") as f:
        for line in f.readlines():
            print(f"wb: {line}")
    with wb.f3.open("r") as f:
        for line in f.readlines():
            print(f"wb: {line}")
