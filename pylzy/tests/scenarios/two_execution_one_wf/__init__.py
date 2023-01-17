from lzy.api.v1 import op, Lzy


@op
def print_and_return_42() -> int:
    return 42


@op
def print_and_return_13() -> int:
    return 13


workflow_name = "wf"

with Lzy().workflow(name=workflow_name, interactive=False):
    res = print_and_return_42()
    print(res)

with Lzy().workflow(name=workflow_name, interactive=False):
    res = print_and_return_13()
    print(res)
