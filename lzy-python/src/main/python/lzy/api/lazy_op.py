import copyreg
import logging
import multiprocessing.pool
import os
import uuid
from abc import abstractmethod
from typing import Callable, Type, Tuple, Any

import cloudpickle

from lzy.model.slot import Direction
from lzy.model.zygote_python_func import ZygotePythonFunc
from lzy.servant.bash_servant import BashServant
from lzy.servant.servant import Servant


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


def read_from_slot(path, log, box):
    log.info(f"Reading result from {path}")
    with open(path, 'rb') as handle:
        box[0] = cloudpickle.load(handle)
    log.info(f"Read result from {path}; removing slot")
    os.remove(path)


class LzyRemoteOp(LzyOp):
    def __init__(self, func: Callable, input_types: Tuple[type, ...],
                 output_type: type, *args):
        super().__init__(func, input_types, output_type, *args)
        self._deployed = False
        self.rc = None

    def execution_logic(self):
        servant: Servant = BashServant()
        zygote = ZygotePythonFunc(self._func, servant.mount())

        execution_id = str(uuid.uuid4())
        bindings = servant.configure_slots(zygote, execution_id)
        for i, slot in enumerate(bindings.local_slots(Direction.OUTPUT)):
            assert i < 1
            self._log.info(f"Writing argument to slot {slot.name()}")
            with open(servant.get_slot_path(slot), 'wb') as handle:
                cloudpickle.dump(self, handle)
            self._log.info(f"Written argument to slot {slot.name()}")

        slot = bindings.local_slots(Direction.INPUT)[0]
        slot_path = servant.get_slot_path(slot)

        box = [None]
        process = multiprocessing.Process(target=read_from_slot, name='read_from_slot', args=(slot_path, self._log, box))
        process.start()

        self._log.info(f"Run task {execution_id} func={self.func.__name__}")
        rc = servant.run(zygote, bindings)

        process.join()
        self._materialization = box[0]
        self._log.info("Executed task %s for func %s with rc %s", execution_id[:4], self.func.__name__, str(rc))

    def deploy(self):
        self._deployed = True

    def materialize(self) -> Any:
        self._log.info("Materializing function %s", self.func)
        if not self._materialized:
            if self._deployed:
                self._materialization = self.func(*self.args)
            else:
                self.execution_logic()
            self._materialized = True
            self._log.info("Materializing function %s done", self.func)
        else:
            # noinspection PyTypeChecker
            self._log.info("Function %s has been already materialized", self.func)
        return self._materialization

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
