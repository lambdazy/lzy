import os
import uuid

from lzy.api.v1 import LzyRemoteEnv, op


@op
def check_env_var_default_image() -> str:
    return os.environ["CUSTOM_ENV"]


@op(docker_image="lzydock/test-env:custom3")
def check_env_var_custom_image() -> str:
    return os.environ["CUSTOM_ENV"]


WORKFLOW_NAME = "workflow_" + str(uuid.uuid4())


if __name__ == "__main__":

    with LzyRemoteEnv().workflow(name=WORKFLOW_NAME):
        result = check_env_var_custom_image()
        print("Custom env: " + str(result))

    with LzyRemoteEnv().workflow(
        name=WORKFLOW_NAME, docker_image="lzydock/test-env:custom3"
    ):
        result = check_env_var_default_image()
        print("Custom env: " + str(result))
