import os

from lzy.api.v1 import op, Lzy, Env


@op
def repeatable_op(a: int) -> int:
    print(a)
    return a + 1


@op
def just_return_none(a: int, b: int) -> None:
    print(f"a={a},b={b}")
    print(os.environ["LOL"])
    return None


with Lzy().workflow(name="wf", interactive=False, env=Env(env_variables={"LOL": "kek"})):
    a = 39
    for i in range(3):
        a = repeatable_op(a)

    # noinspection PyNoneFunctionAssignment
    res = just_return_none(1, 1)
    print(res)
    print(a)
