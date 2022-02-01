from lzy.api import op, LzyRemoteEnv


@op
def raises() -> int:
    raise RuntimeError("Bad exception")


if __name__ == "__main__":
    with LzyRemoteEnv():
        raises()
