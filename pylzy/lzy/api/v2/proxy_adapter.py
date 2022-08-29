from typing import Any, Type, TypeVar, cast

from lzy._proxy.result import Nothing, Just

from lzy._proxy.automagic import proxy
from lzy.api.v2 import LzyWorkflow
from lzy.api.v2.call import LzyCall
from lzy.api.v2.exceptions import LzyExecutionException

____lzy_proxied = "__lzy_proxied__"


def is_lzy_proxy(obj: Any) -> bool:
    cls = type(obj)
    return hasattr(cls, ____lzy_proxied) and cls.____lzy_proxied


def materialized(obj: Any) -> bool:
    return obj.__lzy_materialized__  # type: ignore


def materialize(obj: Any) -> Any:
    return obj.__lzy_origin__  # type: ignore


def lzy_proxy(entry_id: str, typ: type, wflow: LzyWorkflow) -> Any:
    def materialize():
        data = wflow.owner.runtime.resolve_data(entry_id)
        if isinstance(data, Just):
            return data.value

        wflow.barrier()

        new_data = wflow.owner.runtime.resolve_data(entry_id)
        if isinstance(new_data, Just):
            return new_data.value
        raise LzyExecutionException("Cannot materialize data")

    return proxy(
        materialize,
        typ,  # type: ignore
        cls_attrs={____lzy_proxied: True},
    )
