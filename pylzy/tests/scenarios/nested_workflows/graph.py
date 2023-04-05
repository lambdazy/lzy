from typing import Any

from lzy.api.v1 import Lzy, op


def run_graph(user: str, key: str, obj: Any):
    @op
    def run(a) -> None:
        print(a)

    lzy = Lzy()
    lzy.auth(user=user, key_path=key)

    with lzy.workflow("internal", interactive=False):
        run(obj)
