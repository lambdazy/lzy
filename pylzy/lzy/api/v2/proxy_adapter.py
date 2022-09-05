from typing import TYPE_CHECKING, Any

from lzy._proxy.automagic import proxy
from lzy._proxy.result import Just

if TYPE_CHECKING:
    from lzy.api.v2 import LzyWorkflow

from lzy.api.v2.exceptions import LzyExecutionException

__lzy_proxied = "__lzy_proxied__"
__entry_id = "__lzy_entry_id__"


def is_lzy_proxy(obj: Any) -> bool:
    cls = type(obj)
    return hasattr(cls, __lzy_proxied) and getattr(cls, __lzy_proxied)


def materialized(obj: Any) -> bool:
    return obj.__lzy_materialized__  # type: ignore


def materialize(obj: Any) -> Any:
    return obj.__lzy_origin__  # type: ignore


def lzy_proxy(entry_id: str, typ: type, wflow: "LzyWorkflow") -> Any:
    def materialize():
        data = wflow.owner.snapshot.get_data(entry_id)
        if isinstance(data, Just):
            return data.value

        wflow.barrier()

        new_data = wflow.owner.snapshot.get_data(entry_id)
        if isinstance(new_data, Just):
            return new_data.value
        raise LzyExecutionException("Cannot materialize data")

    return proxy(
        materialize,
        typ,  # type: ignore
        cls_attrs={__lzy_proxied: True, __entry_id: entry_id},
    )
