import os

from lzy.api.v1 import Lzy, op
from localq import two


def is_inside_container() -> bool:
    return os.environ.get("LZY_INNER_CONTAINER") == "true"


@op(docker_image="lzydock/user-test:1.3.1")
def check_env_var_custom_image() -> bool:
    print("op1: " + str(two()))
    return is_inside_container()


@op
def check_env_var_default_image() -> bool:
    print("op2: " + str(two()))
    return is_inside_container()


if __name__ == "__main__":
    lzy = Lzy()

    with lzy.workflow(name="wf", interactive=False):
        result = check_env_var_custom_image()
        print("op1 custom env: " + str(result))

    with lzy.workflow(name="wf", interactive=False, docker_image="lzydock/user-test:1.3.1"):
        result = check_env_var_default_image()
        print("op2 custom env: " + str(result))
