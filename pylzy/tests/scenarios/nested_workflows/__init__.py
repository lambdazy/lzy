import os

from lzy.api.v1 import Lzy, op, USER_ENV, KEY_PATH_ENV
from graph import run_graph
from deps import CustomClass
from lzy.types import File


@op
def run(user: str, key: File, obj: CustomClass) -> None:
    run_graph(user, str(key), obj)


with Lzy().workflow(name="wf", interactive=False):
    run(os.getenv(USER_ENV), File(os.getenv(KEY_PATH_ENV)), CustomClass())
