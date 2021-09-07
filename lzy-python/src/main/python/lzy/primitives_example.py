from lzy.api import LzyEnvironmentBuilder, op


@op
def get_num() -> int:
    return 100


def main():
    leb = LzyEnvironmentBuilder()
    env = leb.build()
    with env:
        num = get_num()
        env.print_relations()
        for i in range(num):
            print(i)
        env.print_relations()


if __name__ == "__main__":
    main()
