from lzy.api.v1 import op, Lzy


@op
def just_return_none() -> None:
    return None

with Lzy().workflow(name="wf5", interactive=False):
    # noinspection PyNoneFunctionAssignment
    res = just_return_none()
    print(res)
