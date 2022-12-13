from typing import TYPE_CHECKING, Any, cast, TypeVar, Type

from lzy.utils.event_loop import LzyEventLoop
from lzy.proxy.automagic import proxy
from lzy.proxy.result import Just, Result, Nothing

if TYPE_CHECKING:
    from lzy.api.v1 import LzyWorkflow

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


T = TypeVar("T")


def lzy_proxy(entry_id: str, typ: Type[T], wflow: "LzyWorkflow", value: Result[T] = Nothing()) -> Any:

    async def __materialize() -> Any:

        if isinstance(value, Just):
            return value.value

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
        lambda: LzyEventLoop.run_async(__materialize()),
        typ,  # type: ignore
        cls_attrs={__lzy_proxied: True, __entry_id: entry_id},
    )