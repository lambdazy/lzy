import copyreg
import logging
import os
import uuid
import time

from abc import abstractmethod, ABC
from pathlib import Path
from typing import Optional, Any, TypeVar, Generic

import cloudpickle

from lzy.api.whiteboard.model import EntryIdGenerator
from lzy.api.result import Just, Nothing, Result
from lzy.model.channel import Channel, Binding, Bindings
from lzy.model.env import PyEnv
from lzy.model.file_slots import create_slot
from lzy.model.return_codes import PyReturnCode, ReturnCode
from lzy.model.signatures import CallSignature, FuncSignature
from lzy.model.slot import Direction, Slot
from lzy.model.zygote import Zygote, Provisioning
from lzy.model.zygote_python_func import ZygotePythonFunc
from lzy.servant.servant_client import ServantClient, Execution


T = TypeVar("T")  # pylint: disable=invalid-name


class LzyOp(Generic[T], ABC):
    def __init__(self, signature: CallSignature[T], return_entry_id: str):
        super().__init__()
        self._sign: CallSignature[T] = signature
        self._return_entry_id = return_entry_id
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

    def return_entry_id(self) -> str:
        return self._return_entry_id

    def __repr__(self):
        return f"{self.__class__.__name__}: signature={self._sign}, " \
               f"materialized={self._materialized}, " \
               f"materialization={self._materialization}"


class LzyLocalOp(LzyOp, Generic[T]):
    def __init__(self, signature: CallSignature[T]):
        super().__init__(signature, str(uuid.uuid4()))

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


class LzyExecutionException(Exception):
    pass


class LzyRemoteOp(LzyOp, Generic[T]):
    def __init__(
        self,
        servant: ServantClient,
        signature: CallSignature[T],
        provisioning: Provisioning = None,
        env: PyEnv = None,
        deployed: bool = False,
        entry_id_generator: Optional[EntryIdGenerator] = None,
        return_entry_id: Optional[str] = None,
    ):
        if (not provisioning or not env) and not deployed:
            raise ValueError("Non-deployed ops must have provisioning and env")

        self._deployed = deployed
        self._servant = servant
        self._env = env
        self._zygote = ZygotePythonFunc(
            signature.func,
            # self._servant.mount(),
            env,
            provisioning,
        )

        if return_entry_id is not None and entry_id_generator is not None:
            raise ValueError("Both entry id and entry id generator are provided")
        elif entry_id_generator is not None:
            return_entry_id = entry_id_generator.generate(self._zygote.return_slot)

        super().__init__(signature, str(return_entry_id))

    @property
    def zygote(self) -> Zygote:
        return self._zygote

    def dump_arguments(self, execution: Execution):
        args = self.signature.args
        param_names = self.signature.func.param_names
        slots = self._zygote.arg_slots
        for value, name, slot in zip(args, param_names, slots):
            self.dump_argument(execution, value, name, slot)

    def dump_argument(self, execution, value, name, slot):
        local_slot = self.resolve_slot(execution, slot)
        self._log.info(f"Writing argument {name} to local slot"
                       f"{local_slot.name}")
        local_slot_path = self._servant.get_slot_path(local_slot)
        self.dump_value_to_slot(local_slot_path, value)
        self._log.info(
            f"Written argument {name} to local slot {local_slot.name}")

    def read_return_value(self, execution: Execution) -> Result[Any]:
        return_slot = self._zygote.return_slot
        return_local_slot = self.resolve_slot(execution, return_slot)
        return_slot_path = self._servant.get_slot_path(return_local_slot)
        self._log.info(f"Reading result from {return_slot_path}")
        return_value = self.read_value_from_slot(return_slot_path)
        if isinstance(return_value, Nothing):
            self._log.error(f"Failed to read result from {return_slot_path}")
        return return_value

    @staticmethod
    def dump_value_to_slot(slot_path: Path, obj: Any):
        with slot_path.open("wb") as handle:
            cloudpickle.dump(obj, handle)
            handle.flush()
            os.fsync(handle.fileno())

    @staticmethod
    def read_value_from_slot(slot_path: Path) -> Result[Any]:
        # noinspection PyBroadException
        try:
            return Just(LzyRemoteOp._read_value_from_slot(slot_path))
        except (OSError, ValueError) as _:
            return Nothing()
        except BaseException as _:  # pylint: disable=broad-except
            return Nothing()

    @staticmethod
    def _read_value_from_slot(slot_path: Path) -> Optional[Any]:
        with slot_path.open("rb") as handle:
            # Wait for slot to become open
            while handle.read(1) is None:
                time.sleep(0)  # Thread.yield
            handle.seek(0)
            value = cloudpickle.load(handle)
        return value

    @staticmethod
    def resolve_slot(execution: Execution, local_slot: Slot) -> Slot:
        slot = execution.bindings().local_slot(local_slot)
        if slot is None:
            raise RuntimeError(f"Slot {local_slot.name} not binded")

        return slot

    @staticmethod
    def _execution_exception_message(
                execution: Execution,
                func: FuncSignature[Any], return_code: int) -> str:

        if return_code == ReturnCode.ENVIRONMENT_INSTALLATION_ERROR.value:
            message = "Failed to install environment on remote machine"
        elif return_code == ReturnCode.EXECUTION_ERROR.value:
            message = "Lzy error"
        else:
            message = "Execution error"
        return LzyRemoteOp._exception(execution, func, return_code, message)

    @staticmethod
    def _exception(execution: Execution, func: FuncSignature[Any],
                   returncode: int, message: str) -> str:
        return (
            f"Task {execution.id()[:4]} failed in func {func.name}"
            f"with rc {returncode} and message: {message}"
        )

    def execution_logic(self) -> T:
        entry_id_mapping = (
            {self._zygote.return_slot: self._return_entry_id}
            if self._return_entry_id else
            None
        )
        execution_id = str(uuid.uuid4())
        self._log.info(f"Running zygote {self._zygote.name}, execution id {execution_id}")
        bindings = []
        try:
            bindings = [
                self._create_binding(execution_id, slot)
                for slot in self._zygote.slots
            ]

            execution = self._servant.run(
                execution_id, self._zygote,
                Bindings(bindings), entry_id_mapping
            )

            self.dump_arguments(execution)
            return_value = self.read_return_value(execution)

            func = self.signature.func

            result = execution.wait_for()
            rc_ = result.returncode
            if rc_ == 0 and return_value is not None:
                self._log.info("Executed task %s for func %s with rc %s",
                               execution.id()[:4], self.signature.func.name, rc_,)
                return return_value.value  # type: ignore

            message = ""
            if rc_ != 0:
                message = self._execution_exception_message(execution, func, rc_)
                self._log.error(f"Execution exception with message: {message}")
            elif isinstance(return_value, Nothing):
                message = "Return value deserialization failure"
                message = self._exception(execution, func, PyReturnCode.DESERIALIZATION_FAILURE.value, message)

            raise LzyExecutionException(message)

        finally:
            for binding in bindings:
                self._destroy_binding(binding)

    def materialize(self) -> Any:
        name = self.signature.func.name
        self._log.info("Materializing function %s", name)
        if not self._materialized:
            if self._deployed:
                self._materialization = self.signature.exec()
            else:
                self._materialization = self.execution_logic()
            self._materialized = True
            self._log.info("Materializing function %s done", name)
        else:
            # noinspection PyTypeChecker
            self._log.info("Function %s has been already materialized", name)
        return self._materialization

    # pylint: disable=too-many-arguments
    @staticmethod
    def restore(
        servant: ServantClient,
        materialized: bool,
        materialization: Any,
        return_entry_id: Optional[str],
        call_s: CallSignature[T],
        provisioning: Provisioning,
        env: PyEnv,
    ):
        op_ = LzyRemoteOp(
            servant,
            call_s,
            provisioning,
            env,
            deployed=False,
            return_entry_id=return_entry_id,
        )
        op_._materialized = materialized  # pylint: disable=protected-access
        op_._materialization = materialization  # pylint: disable=protected-access
        return op_

    @staticmethod
    def reducer(op_: "LzyRemoteOp") -> Any:
        return LzyRemoteOp.restore, (
            # pylint: disable=protected-access
            op_._servant, op_.is_materialized(), op_._materialization,
            op_.return_entry_id(),
            op_.signature,
            op_.zygote.provisioning,
            op_.zygote.env,
        )

    def _create_binding(self, execution_id: str, slot: Slot) -> Binding:
        slot_full_name = "/task/" + execution_id + slot.name
        local_slot = create_slot(slot_full_name, Direction.opposite(slot.direction))
        channel = Channel(':'.join([execution_id, slot.name]))
        self._servant.create_channel(channel)
        self._servant.touch(local_slot, channel)
        return Binding(local_slot, slot, channel)

    def _destroy_binding(self, binding: Binding) -> None:
        self._servant.destroy_channel(binding.channel)


copyreg.dispatch_table[LzyRemoteOp] = LzyRemoteOp.reducer  # type: ignore
