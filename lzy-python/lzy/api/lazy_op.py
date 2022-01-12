import copyreg
import logging
import os
from abc import abstractmethod, ABC
import time
from typing import Callable, Optional, Any, Tuple, Type, TypeVar, Generic

import cloudpickle

from lzy.model.env import PyEnv
from lzy.model.return_codes import PyReturnCode, ReturnCode
from lzy.model.signatures import CallSignature, FuncSignature
from lzy.model.zygote_python_func import ZygotePythonFunc
from lzy.model.zygote import Zygote, Provisioning

from lzy.api.whiteboard.api import EntryIdGenerator

from lzy.servant.servant_client import ServantClient

T = TypeVar('T')


class LzyOp(Generic[T], ABC):
    def __init__(self, signature: CallSignature[T]):
        super().__init__()
        self._sign: CallSignature[T] = signature
        self._materialized: bool = False
        self._materialization: Optional[T] = None
        self._log: logging.Logger = logging.getLogger(str(self.__class__))

    @property
    def signature(self) -> CallSignature[T]:
        return self._sign

    def is_materialized(self) -> bool:
        return self._materialized

    @abstractmethod
    def materialize(self) -> T:
        pass

    @abstractmethod
    def return_entry_id(self) -> Optional[str]:
        pass


class LzyLocalOp(LzyOp, Generic[T]):
    def materialize(self) -> T:
        self._log.info("Materializing function %s", self.signature.func)
        name = self.signature.func.name
        if not self._materialized:
            self._materialization: T = self.signature.exec()
            self._materialized = True
            self._log.info("Materializing function %s done", name)
        else:
            self._log.info("Function %s has been already materialized", name)
        return self._materialization

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
    def __init__(self, servant: ServantClient, signature: CallSignature[T],
                 provisioning: Provisioning = None, env: PyEnv = None,
                 deployed: bool = False,
                 entry_id_generator: Optional[EntryIdGenerator] = None,
                 return_entry_id: Optional[str] = None):
        if (not provisioning or not env) and not deployed:
            raise ValueError('Non-deployed ops must have provisioning and env')

        super().__init__(signature)

        self._deployed = deployed
        self._servant = servant
        self._zygote = ZygotePythonFunc(signature.func, self._servant.mount(),
                                        env, provisioning)

        self._return_entry_id: Optional[str] = return_entry_id
        if entry_id_generator is not None:
            self._return_entry_id = entry_id_generator.generate(self._zygote.return_slot)

    @property
    def zygote(self) -> Zygote:
        return self._zygote

    def execution_logic(self):
        bindings = {self._zygote.return_slot: self._return_entry_id} if self._return_entry_id else None
        execution = self._servant.run(self._zygote, bindings)

        call_s = self.signature
        slots = self._zygote.arg_slots
        for arg, name, slot in zip(call_s.args, call_s.func.param_names, slots):
            local_slot = execution.bindings().local_slot(slot)
            if not local_slot:
                raise RuntimeError(f"Slot {slot.name} not binded")
            self._log.info(
                f"Writing argument {name} to local slot {local_slot.name}")

            with open(self._servant.get_slot_path(local_slot), 'wb') as handle:
                cloudpickle.dump(arg, handle)
                handle.flush()
                os.fsync(handle.fileno())

            self._log.info(
                f"Written argument {name} to local slot {local_slot.name}")

        return_local_slot = execution.bindings().local_slot(
            self._zygote.return_slot)
        if not return_local_slot:
            raise RuntimeError(f"Slot {self._zygote.return_slot.name} not binded")
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
        func = self.signature.func.callable
        if rc:
            if rc == ReturnCode.ENVIRONMENT_INSTALLATION_ERROR.value:
                raise LzyExecutionException(
                    "Failed to install environment on remote machine", func,
                    execution, rc)
            if rc == ReturnCode.EXECUTION_ERROR.value:
                raise LzyExecutionException("Lzy error", func, execution, rc)

            raise LzyExecutionException("Execution error", func, execution, rc)
        elif deserialization_failed:
            raise LzyExecutionException("Return value deserialization failure",
                                        func, execution,
                                        PyReturnCode.DESERIALIZATION_FAILURE)

        self._log.info("Executed task %s for func %s with rc %s",
                       execution.id()[:4], self.signature.func.name, rc)

    def materialize(self) -> Any:
        name = self.signature.func.name
        self._log.info("Materializing function %s", name)
        if not self._materialized:
            if self._deployed:
                self._materialization = self.signature.exec()
            else:
                self.execution_logic()
            self._materialized = True
            self._log.info("Materializing function %s done", name)
        else:
            # noinspection PyTypeChecker
            self._log.info("Function %s has been already materialized", name)
        return self._materialization

    @staticmethod
    def restore(servant: ServantClient, materialized: bool, materialization: Any,
                return_entry_id: Optional[str], call_s: CallSignature[T],
                provisioning: Provisioning, env: PyEnv):
        op = LzyRemoteOp(servant, call_s, provisioning, env,
                         deployed=False, return_entry_id=return_entry_id)
        op._materialized = materialized
        op._materialization = materialization
        return op

    @staticmethod
    def reducer(op: 'LzyRemoteOp') -> Any:
        return LzyRemoteOp.restore, (
            op._servant, op.is_materialized(), op._materialization,
            op.return_entry_id(),
            op.signature,
            op.zygote.provisioning, op.zygote.env)

    def return_entry_id(self) -> Optional[str]:
        return self._return_entry_id


copyreg.dispatch_table[LzyRemoteOp] = LzyRemoteOp.reducer  # type: ignore
