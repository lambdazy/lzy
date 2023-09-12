import os

from lzy.api.v1 import op, Lzy


@op
def repeatable_op(a: int) -> int:
    print(a)
    return a + 1


@op
def just_return_none(a: int, b: int) -> None:
    print(f"a={a},b={b}")
    print(os.environ["LOL"])
    return None


if __name__ == '__main__':
    with Lzy().workflow(name="wf", interactive=False).with_env_vars({"LOL": "kek"}):
        a = 39
        for i in range(3):
            a = repeatable_op(a)

        # noinspection PyNoneFunctionAssignment
        res = just_return_none(1, 1)
        print(res)
        print(a)
