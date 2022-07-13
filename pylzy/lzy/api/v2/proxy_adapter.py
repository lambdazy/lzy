from typing import Any

from lzy.api.v2.api.lzy_call import LzyCall

from lzy._proxy.automagic import proxy

____lzy_proxied = "__lzy_proxied__"


def is_lzy_proxy(obj: Any) -> bool:
    cls = type(obj)
    return hasattr(cls, ____lzy_proxied) and cls.____lzy_proxied


def materialized(obj: Any) -> bool:
    return obj.__lzy_materialized__  # type: ignore


def lzy_proxy(lzy_call: LzyCall):
    def materialize():
        wflow = lzy_call.parent_wflow
        value = wflow.snapshot().get(lzy_call.entry_id)
        if value is not None:
            return value
        wflow.barrier()
        return wflow.snapshot().get(lzy_call.entry_id)

    return proxy(
        materialize,
        lzy_call.signature.func.output_type,
        cls_attrs={____lzy_proxied: True},
    )
