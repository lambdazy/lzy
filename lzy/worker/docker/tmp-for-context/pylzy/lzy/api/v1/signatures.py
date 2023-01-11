from dataclasses import dataclass
from itertools import chain
from typing import (
    Any,
    Callable,
    Dict,
    Generic,
    Iterable,
    Iterator,
    Sequence,
    Tuple,
    Type,
    TypeVar,
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

    @property
    def description(self) -> str:
        # TODO: is it needed?
        if not hasattr(self.callable, "__name__"):
            return repr(self.callable)
        return self.callable.__name__

    def __repr__(self) -> str:
        input_types = ", ".join(f"{n}={t}" for n, t in self.input_types.items())
        return f"{self.callable} {self.name}({input_types}) -> {self.output_types}"


@dataclass
class CallSignature:
    func: FuncSignature
    args: Tuple[Any, ...]
    kwargs: Dict[str, Any]

    def exec(self) -> Any:
        print("Calling: ", self.description)
        return self.func.callable(*self.args, **self.kwargs)

    def named_arguments(self) -> Iterator[Tuple[str, Any]]:
        return chain(zip(self.func.arg_names, self.args), self.kwargs.items())

    @property
    def description(self) -> str:
        return self.func.description  # TODO(artolord) Add arguments description here
