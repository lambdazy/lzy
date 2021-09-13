from typing import Callable, Type, Tuple, Any

from lzy.proxy import Proxy
from lzy.runner import LzyRunner


class LzyOp(Proxy):
    def __init__(self, runner: LzyRunner, func: Callable, typ: Type, *args):
        super().__init__(typ)
        self._runner = runner
        self._func = func
        self._args = args
        self._materialized = False
        self._materialization = None

    def materialize(self) -> Any:
        if not self._materialized:
            self._materialization = self._runner.run(self._func, *self._args)
            self._materialized = True
        return self._materialization

    def is_materialized(self) -> bool:
        return self._materialized

    def func(self) -> Callable:
        return self._func

    def args(self) -> Tuple:
        return self._args

    def call(self, name: str, *args) -> Any:
        return getattr(self.materialize(), name)(*args)
