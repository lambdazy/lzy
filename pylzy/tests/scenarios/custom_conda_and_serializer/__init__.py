import tempfile
import uuid
from pathlib import Path

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
    pylzy_path = Path(__file__).parent.parent.parent.parent.absolute()
    with tempfile.NamedTemporaryFile(mode="w", dir="/tmp", suffix=".yaml") as f:
        f.write(
            "\n".join(
                [
                    "name: custom",
                    "dependencies:",
                    "- python==3.9.7",
                    "- pip",
                    "- pip:",
                    "  - " + str(pylzy_path),
                    "",
                ]
            )
        )
        f.flush()
        lzy = Lzy()
        lzy.serializer_registry.register_serializer(TestStrSerializer())
        with lzy.workflow("wf", interactive=False, conda_yaml_path=f.name):
            result = func("some_str")
            print(result)
