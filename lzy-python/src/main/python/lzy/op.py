import copyreg
import logging
from abc import abstractmethod
from typing import Callable, Type, Tuple, Any

import cloudpickle
import subprocess
import uuid

import os
import sys
import site

from lzy.proxy import Proxy


class LzyOp(Proxy):
    def __init__(self, func: Callable, typ: Type, *args):
        super().__init__(typ)
        self._func = func
        self._args = args

    @abstractmethod
    def materialize(self) -> Any:
        pass

    @abstractmethod
    def is_materialized(self) -> bool:
        pass

    def func(self) -> Callable:
        return self._func

    def args(self) -> Tuple:
        return self._args

    def on_call(self, name: str, *args) -> Any:
        return getattr(self.materialize(), name)(*args)


class LzyLocalOp(LzyOp):
    def __init__(self, func: Callable, typ: Type, *args):
        super().__init__(func, typ, *args)
        self._materialized = False
        self._materialization = None
        self._log = logging.getLogger(str(self.__class__))

    def materialize(self) -> Any:
        self._log.info("Materializing function %s", self.func())
        if not self._materialized:
            self._materialization = self.func()(*self.args())
            self._materialized = True
            self._log.info("Materializing function %s done", self.func())
        else:
            self._log.info("Function %s has been already materialized", self.func())
        return self._materialization

    def is_materialized(self) -> bool:
        return self._materialized


class LzyRemoteOp(LzyOp):
    def __init__(self, func: Callable, typ: Type, *args):
        super().__init__(func, typ, *args)
        self._materialized = False
        self._materialization = None
        self._deployed = False
        self._log = logging.getLogger(str(self.__class__))

    def deploy(self):
        self._deployed = True

    def materialize(self) -> Any:
        self._log.info("Materializing function %s", self.func())
        if not self._materialized:
            if self._deployed:
                self._materialization = self.func()(*self.args())
            else:
                pickle_in_path = '/tmp/' + str(uuid.uuid4())
                pickle_out_path = '/tmp/' + str(uuid.uuid4())
                with open(pickle_in_path, 'wb') as handle:
                    cloudpickle.dump(self, handle)
                process = subprocess.Popen(
                    [sys.executable, site.getsitepackages()[0] + "/lzy/startup.py", pickle_in_path,
                     pickle_out_path])
                rc = process.wait()
                self._log.info("Run process %s for func %s with rc %s", str(process.pid), self.func(), str(rc))
                with open(pickle_out_path, 'rb') as handle:
                    self._materialization = cloudpickle.load(handle)
                os.remove(pickle_in_path)
                os.remove(pickle_out_path)
            self._materialized = True
            self._log.info("Materializing function %s done", self.func())
        else:
            # noinspection PyTypeChecker
            self._log.info("Function %s has been already materialized", self.func())
        return self._materialization

    def is_materialized(self) -> bool:
        return self._materialized

    @staticmethod
    def restore(materialized: bool, materialization: Any, func: Callable, typ: Type, *args):
        op = LzyRemoteOp(func, typ, *args)
        op._materialized = materialized
        op._materialization = materialization
        return op

    @staticmethod
    def reducer(op) -> Any:
        # noinspection PyProtectedMember
        return LzyRemoteOp.restore, (op.is_materialized(), op._materialization, op.func(), op.typ(), *op.args(),)


copyreg.dispatch_table[LzyRemoteOp] = LzyRemoteOp.reducer
