import copyreg
import logging
from abc import abstractmethod
from typing import Callable, Type, Tuple, Any

import cloudpickle

from .server import server


class LzyOp:
    def __init__(self, func: Callable, input_types: Tuple[type, ...],
                 return_type: type, *args):
        super().__init__()
        self._func = func
        self._args = args
        self._output_type = return_type
        self._input_types = input_types

        self._materialized = False
        self._materialization = None

        self._log = logging.getLogger(str(self.__class__))

    @property
    def func(self) -> Callable:
        return self._func

    @property
    def args(self) -> Tuple:
        return self._args

    @property
    def return_type(self):
        return self._output_type

    @property
    def input_types(self):
        return self._input_types

    @abstractmethod
    def materialize(self) -> Any:
        pass

    @abstractmethod
    def is_materialized(self) -> bool:
        pass


class LzyLocalOp(LzyOp):
    def __init__(self, func: Callable, input_types: Tuple[type, ...],
                 return_type: type, *args):
        super().__init__(func, input_types, return_type, *args)

    def materialize(self) -> Any:
        self._log.info("Materializing function %s", self.func)
        if not self._materialized:
            self._materialization = self.func(*self.args)
            self._materialized = True
            self._log.info("Materializing function %s done", self.func)
        else:
            self._log.info("Function %s has been already materialized",
                           self.func)
        return self._materialization

    def is_materialized(self) -> bool:
        return self._materialized


class LzyRemoteOp(LzyOp):
    def __init__(self, func: Callable, input_types: Tuple[type, ...],
                 output_type: type, *args):
        super().__init__(func, input_types, output_type, *args)
        self._deployed = False
        self.rc = None

        self.deploy()

    def deploy(self):
        self.channel = server.create_channel()
        mapping = {
            str(i): arg.channel.output
            for i, arg in enumerate(self.args)
        }
        mapping.update({
            str(len(self.args)): self.channel.input
        })

        self.rc = server.publish(cloudpickle.dumps(self.func), mapping)
        self._deployed = True

    def materialize(self) -> Any:
        self._log.info("Materializing function %s", self.func())
        if not self._materialized:
            self._materialization = self.sink_output()
            self._materialized = True
            self._log.info("Materializing function %s done", self.func())
        else:
            pass
        # noinspection PyTypeChecker
        self._log.info("Function %s has been already materialized", self.func())
        return self._materialization

    def sink_output(self):
        if self.rc is None:
            raise ValueError
        self.rc.wait()
        with self.channel.output.open('rb') as handle:
            obj = cloudpickle.load(handle)
            return obj

    def is_materialized(self) -> bool:
        return self._materialized

    @staticmethod
    def restore(materialized: bool, materialization: Any,
                input_types: Tuple[Type, ...],
                output_types: Type, func: Callable, *args):
        op = LzyRemoteOp(func, input_types, output_types, *args)
        op._materialized = materialized
        op._materialization = materialization
        return op

    @staticmethod
    def reducer(op) -> Any:
        # noinspection PyProtectedMember
        return LzyRemoteOp.restore, (op.is_materialized(), op._materialization,
                                     op.func, op.return_type, *op.args,)


copyreg.dispatch_table[LzyRemoteOp] = LzyRemoteOp.reducer
