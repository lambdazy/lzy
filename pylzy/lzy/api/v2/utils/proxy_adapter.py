from typing import TYPE_CHECKING, Any, cast

from lzy.proxy.automagic import proxy
from lzy.proxy.result import Just

if TYPE_CHECKING:
    from lzy.api.v2 import LzyWorkflow

from lzy.api.v2.exceptions import LzyExecutionException

__lzy_proxied = "__lzy_proxied__"
__entry_id = "__lzy_entry_id__"


def is_lzy_proxy(obj: Any) -> bool:
    cls = type(obj)
    return hasattr(cls, __lzy_proxied) and getattr(cls, __lzy_proxied)


def get_proxy_entry_id(obj: Any) -> str:
    assert is_lzy_proxy(obj)
    cls = type(obj)
    return cast(str, getattr(cls, __entry_id))


def materialized(obj: Any) -> bool:
    return obj.__lzy_materialized__  # type: ignore


def materialize(obj: Any) -> Any:
    return obj.__lzy_origin__  # type: ignore


def lzy_proxy(entry_id: str, typ: type, wflow: "LzyWorkflow") -> Any:
    async def __materialize() -> Any:
        data = await wflow.snapshot.get_data(entry_id)
        if isinstance(data, Just):
            return data.value

        await wflow._barrier()

        new_data = await wflow.snapshot.get_data(entry_id)
        if isinstance(new_data, Just):
            return new_data.value
        raise RuntimeError(
            f"Cannot materialize data with entry id {entry_id} from workflow {wflow.name}"
        )

    return proxy(
        lambda: wflow._run_async(__materialize()),
        typ,  # type: ignore
        cls_attrs={__lzy_proxied: True, __entry_id: entry_id},
    )
