from lzy.api import op, LzyEnv


@op
def raises() -> int:
    raise RuntimeError("Bad exception")


if __name__ == "__main__":
    with LzyEnv(user="ArtoLord", private_key_path="/home/artolord/.ssh/public.pem"):
        try:
            raises()
            exit(1)
        except RuntimeError as e:
            exit(0)
