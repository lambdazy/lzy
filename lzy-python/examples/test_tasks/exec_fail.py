from lzy.api import op, LzyEnv


@op
def raises() -> int:
    raise RuntimeError("Bad exception")


if __name__ == "__main__":
    with LzyEnv(user="phil"):
        raises()
