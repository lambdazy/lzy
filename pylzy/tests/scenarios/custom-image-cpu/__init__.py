import os

from lzy.api.v1 import Lzy, op
from localq import two


@op
def check_env_var_default_image() -> str:
    print("op1: " + str(two()))
    return os.environ["CUSTOM_ENV"]


@op(docker_image="lzydock/user-test:custom-1")
def check_env_var_custom_image() -> str:
    print("op2: " + str(two()))
    return os.environ["CUSTOM_ENV"]


if __name__ == "__main__":
    lzy = Lzy()

    with lzy.workflow(name="wf", interactive=False):
        result = check_env_var_custom_image()
        print("op1: custom env: " + str(result))

    with lzy.workflow(name="wf", interactive=False, docker_image="lzydock/user-test:custom-1"):
        result = check_env_var_default_image()
        print("op2: custom env: " + str(result))
