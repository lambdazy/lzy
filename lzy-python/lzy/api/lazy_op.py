import copyreg
import inspect
import logging
import os
from abc import abstractmethod, ABC
import time
from typing import Callable, Optional, Type, Tuple, Any, TypeVar, Generic

import cloudpickle
from lzy.api.whiteboard.api import EntryIdGenerator

from lzy.model.env import PyEnv
from lzy.model.zygote import Provisioning
from lzy.model.zygote_python_func import ZygotePythonFunc
from lzy.servant.servant_client import ServantClient
from lzy.model.return_codes import ReturnCode, PyReturnCode

T = TypeVar('T')


class LzyOp(Generic[T], ABC):
    def __init__(self, func: Callable[..., T], input_types: Tuple[type, ...],
                 return_type: Type[T], args: Tuple[Any, ...]):
        super().__init__()
        self._func = func
        self._args = args
        self._return_type = return_type
        self._arg_types = input_types

        self._materialized = False
        self._materialization: Optional[T] = None

        self._log = logging.getLogger(str(self.__class__))

    @property
    def func(self) -> Callable[..., T]:
        return self._func

    @property
    def args(self) -> Tuple[Any, ...]:
        return self._args

    @property
    def return_type(self) -> Type[T]:
        return self._return_type

    @property
    def input_types(self) -> Tuple[type, ...]:
        return self._arg_types

    @abstractmethod
    def materialize(self) -> T:
        pass

    @abstractmethod
    def is_materialized(self) -> bool:
        pass

    @abstractmethod
    def return_entry_id(self) -> Optional[str]:
        pass


class LzyLocalOp(LzyOp, Generic[T]):
    def __init__(self, func: Callable[..., T], input_types: Tuple[type, ...],
                 return_type: Type[T], args: Tuple[Any, ...]):
        super().__init__(func, input_types, return_type, args)

    def materialize(self) -> T:
        self._log.info("Materializing function %s", self.func)
        if not self._materialized:
            self._materialization: T = self.func(*self.args)
            self._materialized = True
            self._log.info("Materializing function %s done", self.func)
        else:
            self._log.info("Function %s has been already materialized",
                           self.func)
        return self._materialization

    def is_materialized(self) -> bool:
        return self._materialized
    
    def return_entry_id(self) -> Optional[str]:
        return None


class LzyExecutionException(Exception):
    def __init__(self, message, func, execution, rc):
        super().__init__(message, func, execution, rc)
        self.message = message
        self.func = func
        self.execution = execution
        self.rc = rc

    def __str__(self):
        return f"Task {self.execution.id()[:4]} failed " \
               f"in func {self.func.__name__} " \
               f"with rc {self.rc} " \
               f"and message: {self.message}"


class LzyRemoteOp(LzyOp, Generic[T]):
    def __init__(
        self, servant: ServantClient, func: Callable,
        input_types: Tuple[type, ...],
        output_type: Type[T], provisioning: Provisioning = None, env: PyEnv = None,
        deployed: bool = False,
        args: Tuple[Any, ...] = (),
        entry_id_generator: Optional[EntryIdGenerator] = None,
        return_entry_id: Optional[str] = None
    ):
        super().__init__(func, input_types, output_type, args)
        self._deployed = deployed
        self._servant = servant
        self._provisioning = provisioning
        self._env = env
        if (not provisioning or not env) and not deployed:
            raise ValueError('Non-deployed ops must have provisioning and env')
        self._zygote = ZygotePythonFunc(func, input_types, output_type, self._servant.mount(), env, provisioning)
        if entry_id_generator is not None:
            self._return_entry_id: Optional[str] = entry_id_generator.generate(self._zygote.return_slot())
        else:
            self._return_entry_id = return_entry_id

    def execution_logic(self):
        bindings = {self._zygote.return_slot(): self._return_entry_id} if self._return_entry_id else None
        execution = self._servant.run(self._zygote, bindings)
        arg_slots = self._zygote.arg_slots()
        arg_names = inspect.getfullargspec(self._func).args
        for i in range(len(self._args)):
            local_slot = execution.bindings().local_slot(arg_slots[i])
            if not local_slot:
                raise RuntimeError(f"Slot {arg_slots[i].name} not binded")
            self._log.info(f"Writing argument {arg_names[i]} to local slot {local_slot.name()}")
            with open(self._servant.get_slot_path(local_slot), 'wb') as handle:
                cloudpickle.dump(self._args[i], handle)
                handle.flush()
                os.fsync(handle.fileno())
            self._log.info(
                f"Written argument {arg_names[i]} to local slot {local_slot.name()}")

        return_local_slot = execution.bindings().local_slot(
            self._zygote.return_slot())
        if not return_local_slot:
            raise RuntimeError(f"Slot {self._zygote.return_slot().name} not binded")
        return_slot_path = self._servant.get_slot_path(return_local_slot)
        self._log.info(f"Reading result from {return_slot_path}")

        deserialization_failed: bool = False
        # noinspection PyBroadException
        try:
            with open(return_slot_path, 'rb') as handle:
                # Wait for slot become open
                while handle.read(1) is None:
                    time.sleep(0)  # Thread.yield
                handle.seek(0)
                self._materialization = cloudpickle.load(handle)
            self._log.info(f"Read result from {return_slot_path}")
        except Exception as e:
            self._log.error(f"Failed to read result from {return_slot_path}\n{e}")
            deserialization_failed = True

        result = execution.wait_for()
        rc = result.rc()
        if rc:
            if rc == ReturnCode.ENVIRONMENT_INSTALLATION_ERROR.value:
                raise LzyExecutionException("Failed to install environment on remote machine", self.func, execution, rc)
            if rc == ReturnCode.EXECUTION_ERROR.value:
                raise LzyExecutionException("Lzy error", self.func, execution, rc)

            raise LzyExecutionException("Execution error", self.func, execution, rc)
        elif deserialization_failed:
            raise LzyExecutionException("Return value deserialization failure", self.func, execution,
                                        PyReturnCode.DESERIALIZATION_FAILURE)

        self._log.info("Executed task %s for func %s with rc %s",
                       execution.id()[:4], self.func.__name__, rc)

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
                return_entry_id: Optional[str],
                input_types: Tuple[Type, ...], output_types: Type[T],
                func: Callable, provisioning: Provisioning, env: PyEnv, *args: Tuple[Any, ...]):
        op = LzyRemoteOp(servant, func, input_types, output_types, provisioning, env, deployed=False, args=args, return_entry_id=return_entry_id)
        op._materialized = materialized
        op._materialization = materialization
        return op

    @staticmethod
    def reducer(op: 'LzyRemoteOp') -> Any:
        # noinspection PyProtectedMember
        return LzyRemoteOp.restore, (
            op._servant, op.is_materialized(), op._materialization,
            op.return_entry_id(),
            op.input_types, op.return_type,
            op.func, op._provisioning, op._env, *op.args)
    
    def return_entry_id(self) -> Optional[str]:
        return self._return_entry_id


copyreg.dispatch_table[LzyRemoteOp] = LzyRemoteOp.reducer  # type: ignore
