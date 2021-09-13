import copyreg
import logging
from abc import abstractmethod
from typing import Callable, Type, Tuple, Any

import cloudpickle
import subprocess
import uuid

import os
import sys

from lzy.proxy import Proxy


class LzyOp(Proxy):
    def __init__(self, runner, func: Callable, typ: Type, *args):
        super().__init__(typ)
        self._runner = runner
        self._func = func
        self._args = args
        self._materialized = False
        self._materialization = None

    def materialize(self, delegate: bool = True) -> Any:
        if not self._materialized:
            if delegate:
                self._materialization = self._runner.run(self)
            else:
                self._materialization = self.func()(*self.args())
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

    def runner(self):  # -> LzyRunner
        return self._runner

    @staticmethod
    def restore(runner, materialized: bool, materialization: Any, func: Callable, typ: Type, *args):
        op = LzyOp(runner, func, typ, *args)
        op._materialized = materialized
        op._materialization = materialization
        return op

    @staticmethod
    def reducer(op) -> Any:
        # noinspection PyProtectedMember
        return LzyOp.restore, (op.runner(), op.is_materialized(), op._materialization, op.func(), op.typ(), *op.args(),)


class LzyRunner:
    @abstractmethod
    def run(self, op: LzyOp) -> Any:
        pass


class LocalLzyRunner(LzyRunner):
    def run(self, op: LzyOp) -> Any:
        return op.func()(*op.args())


class ProcessLzyRunner(LzyRunner):
    def __init__(self):
        super().__init__()
        self._log = logging.getLogger(str(self.__class__))

    def run(self, op: LzyOp) -> Any:
        copyreg.dispatch_table[LzyOp] = LzyOp.reducer
        pickle_in_path = '/tmp/' + str(uuid.uuid4())
        pickle_out_path = '/tmp/' + str(uuid.uuid4())
        with open(pickle_in_path, 'wb') as handle:
            cloudpickle.dump(op, handle)
        process = subprocess.Popen(
            [sys.executable, "/anaconda3/lib/python3.7/site-packages/lzy/startup.py", pickle_in_path, pickle_out_path])
        rc = process.wait()
        # noinspection PyTypeChecker
        self._log.info("Run process %s for func %s with rc %s", str(process.pid), str(op.func()), str(rc))
        with open(pickle_out_path, 'rb') as handle:
            result = cloudpickle.load(handle)
        os.remove(pickle_in_path)
        os.remove(pickle_out_path)
        del copyreg.dispatch_table[LzyOp]
        return result
