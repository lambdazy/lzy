from lzy.api.v1 import op, Lzy


@op(cache=True)
def foo_with_print(name: str, value: int) -> str:
    print("foo was called")
    return f"{name} is {value}"


@op(cache=False)
def bar_with_print(message: str) -> str:
    print("bar was called")
    return f"message from bar: {message}"


workflow_name = "wf"

with Lzy().workflow(name=workflow_name, interactive=False):
    n = "number"
    v = 42
    mes_1 = bar_with_print(foo_with_print(n, v))
    print(mes_1)
    mes_2 = bar_with_print(foo_with_print(n, v))
    print(mes_2)
