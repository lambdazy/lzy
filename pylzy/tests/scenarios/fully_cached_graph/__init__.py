from lzy.api.v1 import op, Lzy


@op(cache=True, version="1.0")
def foo_with_print(name: str, value: int) -> str:
    print("foo was called")
    return f"{name} is {value}"


@op(cache=True, version="1.0")
def bar_with_print(message: str) -> str:
    print("bar was called")
    return f"message from bar: {message}"


workflow_name = "wf"

with Lzy().workflow(name=workflow_name, interactive=False):
    n = "number"
    v = 42
    mes = bar_with_print(foo_with_print(n, v))
    print(mes)

with Lzy().workflow(name=workflow_name, interactive=False):
    n = "number"
    v = 42
    mes = bar_with_print(foo_with_print(n, v))
    print(f"again -- {mes}")
