import os

from lzy.api.v1 import op, Lzy, Env


@op
def just_return_none(a: int, b: int) -> None:
    print(f"a={a},b={b}")
    print(os.environ["LOL"])
    return None


with Lzy().workflow(name="wf", interactive=False, env=Env(env_variables={"LOL": "kek"})):
    # noinspection PyNoneFunctionAssignment
    res = just_return_none(1, 1)
    print(res)
