from dataclasses import dataclass

from typing import TypeVar, Generic, Callable, Dict, Type, Tuple, Iterable

from lzy.v2.api.env.env import Env
from lzy.v2.api.provisioning import Provisioning

T = TypeVar("T")  # pylint: disable=invalid-name


@dataclass
class LzyOp(Generic[T]):
    env: Env
    provisioning: Provisioning

    callable: Callable[..., T]
    input_types: Dict[str, type]
    output_type: Type[T]
    arg_names: Tuple[str, ...]
    kwarg_names: Tuple[str, ...]

    @property
    def param_names(self) -> Iterable[str]:
        return list(self.input_types.keys())

    @property
    def name(self) -> str:
        return self.callable.__name__

    @property
    def description(self) -> str:
        if not hasattr(self.callable, "__name__"):
            return repr(self.callable)
        return self.callable.__name__

    def __repr__(self) -> str:
        input_types = ", ".join(f"{n}={t}" for n, t in self.input_types.items())
        return f"{self.callable} {self.name}({input_types}) -> {self.output_type}"
