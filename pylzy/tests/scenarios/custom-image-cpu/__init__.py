import os

from lzy.api.v1 import Lzy, op


@op
def check_env_var_default_image() -> str:
    return os.environ["CUSTOM_ENV"]


@op(docker_image="lzydock/test-env:custom3")
def check_env_var_custom_image() -> str:
    return os.environ["CUSTOM_ENV"]


if __name__ == "__main__":
    lzy = Lzy()

    with lzy.workflow(name="wf", interactive=False):
        result = check_env_var_custom_image()
        print("Custom env: " + str(result))

    with lzy.workflow(name="wf", interactive=False, docker_image="lzydock/test-env:custom3"):
        result = check_env_var_default_image()
        print("Custom env: " + str(result))
