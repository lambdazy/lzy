from lzy.api.v1 import op, Lzy


@op(cache=True, version="1.0")
def raises() -> None:
    print("exception was raised")
    raise ValueError("test")


if __name__ == '__main__':
    workflow_name = "wf"

    try:
        with Lzy().workflow(name=workflow_name, interactive=False):
            raises()
    except Exception as e:
        pass

    with Lzy().workflow(name=workflow_name, interactive=False):
        raises()
