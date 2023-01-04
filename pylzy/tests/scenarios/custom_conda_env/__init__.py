import tempfile
import uuid
from pathlib import Path

from lzy.api.v1 import Lzy, op


@op
def func() -> None:
    print("DONE")


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
        with Lzy().workflow("wf", interactive=False, conda_yaml_path=f.name):
            func()
