import tempfile
from lzy.api import op, LzyEnv


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

        with LzyEnv(user='phil', yaml_path=f.name):
            main()
