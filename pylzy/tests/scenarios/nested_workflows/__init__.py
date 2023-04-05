import os

from lzy.api.v1 import Lzy, op, USER_ENV, KEY_PATH_ENV
from graph import run_graph
from deps import CustomClass
from lzy.types import File


@op
def run(user: str, key: File, obj: CustomClass) -> None:
    print("Run user: " + user)
    print("Run key path: " + str(key))
    run_graph(user, str(key), obj)


with Lzy().workflow(name="wf", interactive=False):
    user_env = os.getenv(USER_ENV)
    key_path = os.getenv(KEY_PATH_ENV)
    print("User: " + user_env)
    print("Key path: " + key_path)
    run(user_env, File(key_path), CustomClass())
