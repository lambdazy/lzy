from lzy.api.v1 import op, Lzy


@op
def just_return_none(a: int, b: int) -> None:
    print(f"a={a},b={b}")
    return None


with Lzy().workflow(name="wf", interactive=False):
    # noinspection PyNoneFunctionAssignment
    res = just_return_none(1, 1)
    print(res)
