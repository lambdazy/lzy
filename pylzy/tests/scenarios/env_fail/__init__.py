import tempfile
import uuid

from lzy.api.v1 import Lzy, op, auto_python_env


@auto_python_env(additional_pypi_packages={'lol_kek': '123'})
@op
def main() -> int:
    return 0


WORKFLOW_NAME = "workflow_" + str(uuid.uuid4())

if __name__ == "__main__":
    with Lzy().workflow("wf", interactive=False):
        main()
