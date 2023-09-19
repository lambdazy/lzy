import uuid

from lzy.api.v1 import Lzy, op
from serializer import TestStrSerializer


class Kek:
    pass


@op
def func(s: str) -> str:
    print(s)
    return "some_string"


WORKFLOW_NAME = "workflow_" + str(uuid.uuid4())

if __name__ == "__main__":
    lzy = Lzy()
    lzy.serializer_registry.register_serializer(TestStrSerializer())
    with lzy.workflow("wf", interactive=False):
        result = func("some_str")
        print(result)
