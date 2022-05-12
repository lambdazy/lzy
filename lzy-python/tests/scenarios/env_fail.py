import tempfile
from pathlib import Path
import uuid

from lzy.v1.api.v1 import op, LzyRemoteEnv


@op
def main() -> int:
    return 0


WORKFLOW_NAME = "workflow_" + str(uuid.uuid4())

if __name__ == "__main__":
    with tempfile.NamedTemporaryFile(mode="w", dir="/tmp", suffix=".yaml") as f:
        f.write(
            '\n'.join([
                "name: py39",
                "dependencies:",
                "- python==3.9.7",
                "- pip",
                "- pip:",
                "  - kekreader==5653749",
                ""
            ])
        )
        f.flush()

        with LzyRemoteEnv().workflow(name=WORKFLOW_NAME, conda_yaml_path=Path(f.name)):
            main()
