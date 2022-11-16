import uuid

from lzy.api.v2 import op, Lzy


@op
def raises() -> int:
    raise RuntimeError("Bad exception")

if __name__ == "__main__":
    with Lzy().workflow("wf", interactive=False):
        res = raises()
