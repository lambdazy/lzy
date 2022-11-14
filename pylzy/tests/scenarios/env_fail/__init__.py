import tempfile
import uuid
from pathlib import Path

from lzy.api.v2 import Lzy, op
from lzy.api.v2.remote_grpc.runtime import GrpcRuntime


@op
def main() -> int:
    return 0


WORKFLOW_NAME = "workflow_" + str(uuid.uuid4())

if __name__ == "__main__":
    with tempfile.NamedTemporaryFile(mode="w", dir="/tmp", suffix=".yaml") as f:
        f.write(
            "\n".join(
                [
                    "name: py39",
                    "dependencies:",
                    "- python==3.9.7",
                    "- pip",
                    "- pip:",
                    "  - kekreader==5653749",
                    "",
                ]
            )
        )
        f.flush()

        runtime = GrpcRuntime()
        lzy = Lzy(runtime=runtime)
        with lzy.workflow("wf", interactive=False, conda_yaml_path=f.name):
            main()
