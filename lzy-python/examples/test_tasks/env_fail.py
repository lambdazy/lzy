import tempfile
from pathlib import Path

from lzy.api.env import RunConfig
from lzy.api import op, LzyRemoteEnv


@op
def main() -> int:
    return 0


if __name__ == "__main__":
    with tempfile.NamedTemporaryFile(mode="w", dir="/tmp", suffix=".yaml") as f:
        f.write(
            '\n'.join([
                "name: default",
                "dependencies:",
                "- python==3.7.7",
                "- pip",
                "- pip:",
                "  - kekreader==5653749",
                ""
            ])
        )
        f.flush()

        config = RunConfig(yaml=Path(f.name))
        with LzyRemoteEnv():
            main()
