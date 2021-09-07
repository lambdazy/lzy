from lzy.api import LzyEnvironmentBuilder, op, LzyUtils


@op
def get_num() -> int:
    return 100


def main():
    leb = LzyEnvironmentBuilder()
    env = leb.build()
    with env:
        num = get_num()
        LzyUtils.print_lzy_ops(env.registered_ops())
        for i in range(num):
            print(i)
        LzyUtils.print_lzy_ops(env.registered_ops())


if __name__ == "__main__":
    main()
