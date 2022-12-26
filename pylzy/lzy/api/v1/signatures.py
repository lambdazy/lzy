from dataclasses import dataclass
from itertools import chain
from typing import (
    Any,
    Callable,
    Dict,
    Iterable,
    Iterator,
    Sequence,
    Tuple,
    TypeVar,
    Type,
)

T = TypeVar("T")  # pylint: disable=invalid-name


# TODO: own decorator with default unpacking?
@dataclass
class FuncSignature:
    callable: Callable
    input_types: Dict[str, Type]
    output_types: Sequence[Type]
    arg_names: Tuple[str, ...]
    kwarg_names: Tuple[str, ...]

    @property
    def param_names(self) -> Iterable[str]:
        return list(self.input_types.keys())

    @property
    def name(self) -> str:
        return self.callable.__name__


@dataclass
class CallSignature:
    func: FuncSignature
    args: Tuple[Any, ...]
    kwargs: Dict[str, Any]

    def exec(self) -> Any:
        return self.func.callable(*self.args, **self.kwargs)

    def named_arguments(self) -> Iterator[Tuple[str, Any]]:
        return chain(zip(self.func.arg_names, self.args), self.kwargs.items())
