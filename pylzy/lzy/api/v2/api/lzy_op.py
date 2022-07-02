from dataclasses import dataclass
from typing import Callable, Dict, Generic, Iterable, Tuple, Type, TypeVar

from lzy.proto.bet.priv.v2 import EnvSpec, Provisioning, Zygote

from lzy.api.v2.servant.model.zygote import Zygote

# from lzy.api.v2.api.provisioning import Provisioning
# from lzy.env.env import Env


T = TypeVar("T")  # pylint: disable=invalid-name


@dataclass
class FuncSignature(Generic[T]):
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
        # TODO: is it needed?
        if not hasattr(self.callable, "__name__"):
            return repr(self.callable)
        return self.callable.__name__

    def __repr__(self) -> str:
        input_types = ", ".join(f"{n}={t}" for n, t in self.input_types.items())
        return f"{self.callable} {self.name}({input_types}) -> {self.output_type}"