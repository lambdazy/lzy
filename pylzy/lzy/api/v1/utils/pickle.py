import base64
from typing import Any

import cloudpickle


def pickle(obj: Any) -> str:
    return base64.b64encode(cloudpickle.dumps(obj)).decode("ascii")


def unpickle(base64_str: str) -> Any:
    return cloudpickle.loads(base64.b64decode(base64_str.encode("ascii")))
