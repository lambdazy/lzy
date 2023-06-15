from lzy.api.v1 import op, Lzy


@op(cache=True)
def message_producer(name: str) -> str:
    print("message producer was called")
    return f"My name is {name}"


@op(cache=False)
def bar_with_print(message: str, name: str) -> str:
    print("bar was called")
    return f"message from '{name}' bar: {message}"


if __name__ == '__main__':
    workflow_name = "wf"

    with Lzy().workflow(name=workflow_name, interactive=False):
        n = "Graceful"
        mes_1 = bar_with_print(message_producer(n), n)
        print(mes_1)
        mes_2 = bar_with_print(message_producer(n), n)
        print(mes_2)
