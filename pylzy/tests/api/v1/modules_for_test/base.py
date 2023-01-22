from lzy.api.v1 import op
from .internal.internal import get_internal


@op
def internal_op() -> str:
    return get_internal()
