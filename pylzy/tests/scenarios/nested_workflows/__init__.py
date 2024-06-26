import os

from lzy.api.v1 import Lzy, op
from lzy.types import File
from graph import run_graph
from deps import CustomClass

# NB: is not part of the API, in future there will be "normal" solution for nested graphs
from lzy.core.lzy import USER_ENV, KEY_PATH_ENV, ENDPOINT_ENV, WB_ENDPOINT_ENV


@op
def run(user: str, key: File, endpoint: str, wb_endpoint: str, obj: CustomClass) -> None:
    run_graph(user, str(key), endpoint, wb_endpoint, obj)


if __name__ == '__main__':
    with Lzy().workflow(name="wf", interactive=False):
        user_env = os.getenv(USER_ENV)
        key_path = os.getenv(KEY_PATH_ENV)
        endpoint_env = os.getenv(ENDPOINT_ENV)
        wb_endpoint_env = os.getenv(WB_ENDPOINT_ENV)
        run(user_env, File(key_path), endpoint_env, wb_endpoint_env, CustomClass())
