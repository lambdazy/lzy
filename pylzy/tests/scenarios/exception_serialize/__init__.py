from lzy.api.v1 import op, Lzy


@op
def raises() -> None:
    raise Exception("test")


if __name__ == "__main__":
    with Lzy().workflow("wf", interactive=False):
        raises()
