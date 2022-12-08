from lzy.api.v2 import op, Lzy


@op
def just_return_none() -> None:
    return None

with Lzy().workflow(name="wf", interactive=False):
    # noinspection PyNoneFunctionAssignment
    res = just_return_none()
    print(res)
