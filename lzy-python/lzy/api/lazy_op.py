import copyreg
import inspect
import logging
from abc import abstractmethod
from typing import Callable, Type, Tuple, Any, TypeVar

import cloudpickle

from lzy.model.env import PyEnv
from lzy.model.zygote import Provisioning
from lzy.model.zygote_python_func import ZygotePythonFunc
from lzy.servant.servant_client import ServantClient

T = TypeVar('T')


class LzyOp:
    def __init__(self, func: Callable, input_types: Tuple[type, ...],
                 return_type: Type[T], args: Tuple[Any, ...]):
        super().__init__()
        self._func = func
        self._args = args
        self._return_type = return_type
        self._arg_types = input_types

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
    def return_type(self) -> type:
        return self._return_type

    @property
    def input_types(self) -> Tuple[type, ...]:
        return self._arg_types

    @abstractmethod
    def materialize(self) -> Any:
        pass

    @abstractmethod
    def is_materialized(self) -> bool:
        pass


class LzyLocalOp(LzyOp):
    def __init__(self, func: Callable, input_types: Tuple[type, ...],
                 return_type: Type[T], args: Tuple[Any, ...]):
        super().__init__(func, input_types, return_type, args)

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
    def __init__(self, servant: ServantClient, func: Callable,
                 input_types: Tuple[type, ...],
                 output_type: Type[T], provisioning: Provisioning = None, env: PyEnv = None,
                 deployed: bool = False,
                 args: Tuple[Any, ...] = ()):
        super().__init__(func, input_types, output_type, args)
        self._deployed = deployed
        self._servant = servant
        self._provisioning = provisioning
        self._env = env
        if (not provisioning or not env) and not deployed:
            raise ValueError('Non-deployed ops must have provisioning and env')
        self._zygote = ZygotePythonFunc(func, input_types, output_type, self._servant.mount(), env, provisioning)

    def execution_logic(self):
        execution = self._servant.run(self._zygote)
        arg_slots = self._zygote.arg_slots()
        arg_names = inspect.getfullargspec(self._func).args
        for i in range(len(self._args)):
            local_slot = execution.bindings().local_slot(arg_slots[i])
            self._log.info(f"Writing argument {arg_names[i]} to local slot {local_slot.name()}")
            with open(self._servant.get_slot_path(local_slot), 'wb') as handle:
                cloudpickle.dump(self._args[i], handle)
            self._log.info(
                f"Written argument {arg_names[i]} to local slot {local_slot.name()}")

        return_local_slot = execution.bindings().local_slot(
            self._zygote.return_slot())
        return_slot_path = self._servant.get_slot_path(return_local_slot)
        self._log.info(f"Reading result from {return_slot_path}")
        with open(return_slot_path, 'rb') as handle:
            self._materialization = cloudpickle.load(handle)
        self._log.info(f"Read result from {return_slot_path}")

        result = execution.wait_for()
        self._log.info("Executed task %s for func %s with rc %s",
                       execution.id()[:4], self.func.__name__, result.rc())

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
            self._log.info("Function %s has been already materialized",
                           self.func)
        return self._materialization

    def is_materialized(self) -> bool:
        return self._materialized

    @staticmethod
    def restore(servant: ServantClient, materialized: bool, materialization: Any,
                input_types: Tuple[Type, ...], output_types: Type[T],
                func: Callable, provisioning: Provisioning, env: PyEnv, *args: Tuple[Any, ...]):
        op = LzyRemoteOp(servant, func, input_types, output_types, provisioning, env, deployed=False, args=args)
        op._materialized = materialized
        op._materialization = materialization
        return op

    @staticmethod
    def reducer(op) -> Any:
        # noinspection PyProtectedMember
        return LzyRemoteOp.restore, (
            op._servant_client, op.is_materialized(), op._materialization,
            op.input_types, op.return_type,
            op.func, op._provisioning, op._env, *op.args)


copyreg.dispatch_table[LzyRemoteOp] = LzyRemoteOp.reducer
