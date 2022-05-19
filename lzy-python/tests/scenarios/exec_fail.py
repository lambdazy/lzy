from lzy.v1.api import op
import uuid

from lzy.v1.api.env import LzyRemoteEnv


@op
def raises() -> int:
    raise RuntimeError("Bad exception")


WORKFLOW_NAME = "workflow_" + str(uuid.uuid4())

if __name__ == "__main__":
    with LzyRemoteEnv().workflow(name=WORKFLOW_NAME):
        raises()
