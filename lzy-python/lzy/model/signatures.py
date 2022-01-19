import inspect
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Generic, Iterable, Iterator, Optional, Tuple, Type, \
    Callable, TypeVar

T = TypeVar('T')


# TODO: own decorator with default unpacking?
@dataclass
class FuncSignature(Generic[T]):
    callable: Callable[..., T]
    input_types: Tuple[type, ...]
    output_type: Type[T]

    def __post_init__(self):
        self.argspec = inspect.getfullargspec(self.callable)

    @property
    def param_names(self) -> Iterable[str]:
        return list(self.argspec.args)

    @property
    def name(self) -> str:
        return self.callable.__name__

    @property
    def description(self) -> str:
        # TODO: is it needed?
        if not hasattr(self.callable, '__name__'):
            return repr(self.callable)
        return self.callable.__name__

    def __repr__(self) -> str:
        input_types = ", ".join(str(t) for t in self.input_types)
        return f'{self.callable} {self.name}({input_types}) -> {self.output_type}'


@dataclass
class CallSignature(Generic[T]):
    func: FuncSignature[T]
    args: Tuple[Any, ...]

    def exec(self) -> T:
        print("Calling: ", self.description)
        return self.func.callable(*self.args)

    @property
    def description(self) -> str:
        return f"{self.func.description} with args {self.args}"


def param_files(func_s: FuncSignature, prefix: Optional[Path] = None) -> \
        Iterator[Path]:
    prefix = prefix or Path('/')
    return (prefix / func_s.name / name for name in func_s.param_names)


def return_file(func_s: FuncSignature, prefix: Optional[Path] = None) -> Path:
    prefix = prefix or Path('/')
    return prefix / func_s.name / "return"
