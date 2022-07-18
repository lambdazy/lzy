from dataclasses import dataclass
from itertools import chain
from typing import (
    Any,
    Callable,
    Dict,
    Generic,
    Iterable,
    Iterator,
    Tuple,
    Type,
    TypeVar, List, Sequence,
)

T = TypeVar("T")  # pylint: disable=invalid-name


# TODO: own decorator with default unpacking?
@dataclass
class FuncSignature(Generic[T]):
    callable: Callable[..., T]
    input_types: Dict[str, type]
    output_types: Sequence[Type[T]]
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
class CallSignature(Generic[T]):
    func: FuncSignature[T]
    args: Tuple[Any, ...]
    kwargs: Dict[str, Any]

    def exec(self) -> T:
        print("Calling: ", self.description)
        return self.func.callable(*self.args, **self.kwargs)

    def named_arguments(self) -> Iterator[Tuple[str, Any]]:
        for name, arg in chain(
            zip(self.func.arg_names, self.args), self.kwargs.items()
        ):
            yield name, arg

    @property
    def description(self) -> str:
        return self.func.description  # TODO(artolord) Add arguments description here
