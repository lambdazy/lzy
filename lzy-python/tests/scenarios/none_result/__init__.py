import uuid

from lzy.api.v1 import op
from lzy.api.v1.env import LzyRemoteEnv


@op
def just_return_none() -> None:
    return None


WORKFLOW_NAME = "workflow_" + str(uuid.uuid4())

with LzyRemoteEnv().workflow(name=WORKFLOW_NAME):
    res = just_return_none()

print(res)
