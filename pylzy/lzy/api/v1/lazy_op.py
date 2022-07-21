import copyreg
import dataclasses
import logging
import os
import time
import uuid
from abc import ABC, abstractmethod
from typing import Any, Generic, Iterable, List, Optional, Tuple, TypeVar, Union, Type, cast

from pure_protobuf.dataclasses_ import Message, load  # type: ignore

from lzy.api.v1.cache_policy import CachePolicy
from lzy.api.v1.servant.channel_manager import ChannelManager
from lzy.api.v1.servant.model.channel import Binding, Bindings
from lzy.api.v1.servant.model.env import Env, BaseEnv, AuxEnv
from lzy.api.v1.servant.model.execution import (
    Execution,
    ExecutionDescription,
    ExecutionValue,
    InputExecutionValue,
)
from lzy.api.v1.servant.model.return_codes import ReturnCode
from lzy.api.v1.servant.model.slot import DataSchema, pickle_type
from lzy.api.v1.servant.model.zygote import Provisioning, Zygote
from lzy.api.v1.servant.model.zygote_python_func import ZygotePythonFunc
from lzy.api.v1.servant.servant_client import ServantClient
from lzy.api.v1.signatures import CallSignature, FuncSignature
from lzy.api.v1.utils import LzyExecutionException, is_lazy_proxy
from lzy.api.v1.whiteboard.model import EntryIdGenerator, UUIDEntryIdGenerator
from lzy.serialization.hasher import Hasher
from lzy.serialization.serializer import FileSerializer, MemBytesSerializer

T = TypeVar("T")  # pylint: disable=invalid-name


class LzyReturnValue(Generic[T]):
    """
    Class that represents return value of LzyOp
    """
    def __init__(self, op: 'LzyOp', index: int, entry_id: str, typ: Type[T]):
        self.__op = op
        self.__entry_id = entry_id
        self.__type = typ
        self.__index = index

    def materialize(self) -> T:
        data = self.__op.materialize()
        return cast(T, data[self.__index])

    def execute(self):
        self.__op.execute()

    @property
    def entry_id(self) -> str:
        return self.__entry_id

    @property
    def type(self) -> Type[T]:
        return self.__type

    @property
    def op(self) -> 'LzyOp':
        return self.__op

    def change_entry_id(self, entry_id: str):
        self.__entry_id = entry_id


class LzyOp(ABC):
    def __init__(self, signature: CallSignature[Tuple], entry_id_generator: EntryIdGenerator):
        super().__init__()
        self._sign: CallSignature[Tuple] = signature
        self._materialized: bool = False
        self._executed: bool = False
        self._materialization: Optional[Tuple] = None
        self._log: logging.Logger = logging.getLogger(str(self.__class__))
        self._return_values = tuple(
            LzyReturnValue(self, num, entry_id_generator.generate(os.path.join("return", str(num))), typ)
            for num, typ in enumerate(signature.func.output_types)
        )

    @property
    def signature(self) -> CallSignature[Tuple]:
        return self._sign

    def is_materialized(self) -> bool:
        return self._materialized

    @abstractmethod
    def materialize(self) -> Tuple:
        pass

    @abstractmethod
    def execute(self):
        pass

    def return_values(self) -> Tuple[LzyReturnValue, ...]:
        return self._return_values

    def __repr__(self):
        return (
            f"{self.__class__.__name__}: signature={self._sign}, "
            f"materialized={self._materialized}, "
            f"materialization={self._materialization}"
        )


class LzyLocalOp(LzyOp):
    def __init__(self, signature: CallSignature[Tuple]):
        super().__init__(signature, UUIDEntryIdGenerator("snapshot"))

    def materialize(self) -> Tuple:
        self._log.info("Materializing function %s", self.signature.func)
        name = self.signature.func.name
        if not self._materialized:
            self.execute()
            self._log.info("Materializing function %s done", name)
        else:
            self._log.info("Function %s has been already materialized", name)
        if len(self.return_values()) > 1:
            return self._materialization
        return tuple((self._materialization,))

    def execute(self):
        if self._executed:
            return
        self._materialization: T = self.signature.exec()
        self._materialized = True


class LzyRemoteOp(LzyOp):
    def __init__(
        self,
        servant: ServantClient,
        signature: CallSignature[Tuple],
        snapshot_id: str,
        entry_id_generator: EntryIdGenerator,
        mem_serializer: MemBytesSerializer,
        file_serializer: FileSerializer,
        hasher: Hasher,
        provisioning: Optional[Provisioning] = None,
        base_env: Optional[BaseEnv] = None,
        pyenv: Optional[AuxEnv] = None,
        deployed: bool = False,
        channel_manager: Optional[ChannelManager] = None,
        cache_policy: CachePolicy = CachePolicy.IGNORE,
    ):
        if (not provisioning or not pyenv) and not deployed:
            raise ValueError("Non-deployed ops must have provisioning and env")

        self._hasher = hasher
        self._mem_serializer = mem_serializer
        self._file_serializer = file_serializer
        self._deployed = deployed
        self._servant = servant
        self._pyenv = pyenv
        self._snapshot_id: str = snapshot_id
        self._cache_policy = cache_policy

        if not deployed and not channel_manager:
            raise ValueError("ChannelManager not provided")

        self._channel_manager: ChannelManager = channel_manager  # type: ignore

        for input_type in signature.func.input_types.values():
            if issubclass(input_type, Message):
                setattr(input_type, "LZY_MESSAGE", "LZY_WB_MESSAGE")

        output_types = signature.func.output_types
        for output_type in output_types:
            if issubclass(output_type, Message):
                setattr(output_type, "LZY_MESSAGE", "LZY_WB_MESSAGE")

        self._zygote = ZygotePythonFunc(
            mem_serializer, signature.func, Env(base_env=base_env, aux_env=pyenv), provisioning
        )

        self._entry_id_generator = entry_id_generator

        super().__init__(
            signature, entry_id_generator
        )

    @property
    def zygote(self) -> Zygote:
        return self._zygote

    def dump_arguments(self, args: Iterable[Tuple[str, Any]]):
        for entry_id, obj in args:
            data_schema = DataSchema.generate_schema(type(obj))
            path = self._channel_manager.out_slot(entry_id, data_schema)
            with path.open("wb") as file:
                self._file_serializer.serialize_to_file(obj, file)
                file.flush()
                os.fsync(file.fileno())

    @classmethod
    def _execution_exception_message(
        cls, execution: Execution, func: FuncSignature[Any], return_code: int
    ) -> str:

        if return_code == ReturnCode.ENVIRONMENT_INSTALLATION_ERROR.value:
            message = "Failed to install environment on remote machine"
        elif return_code == ReturnCode.EXECUTION_ERROR.value:
            message = "Lzy error"
        else:
            message = "Execution error"
        return LzyRemoteOp._exception(execution, func, return_code, message)

    @classmethod
    def _exception(
        cls,
        execution: Execution,
        func: FuncSignature[Any],
        returncode: int,
        message: str,
    ) -> str:
        return (
            f"Task {execution.id()[:4]} failed in func {func.name}"
            f"with rc {returncode} and message: {message}"
        )

    @dataclasses.dataclass
    class __EntryId:
        entry_id: str

    def resolve_args(self) -> Iterable[Tuple[str, type, Union[Any, __EntryId]]]:
        for name, arg in self.signature.named_arguments():
            if not is_lazy_proxy(arg):
                yield name, type(arg), arg
                continue

            # noinspection PyProtectedMember
            op: LzyReturnValue = arg._op
            op.execute()
            entry_id = self.__EntryId(op.entry_id)
            yield name, op.type, entry_id

    def execution_logic(self):
        execution_id = str(uuid.uuid4())
        self._log.info(
            f"Running zygote {self._zygote.name}, execution id {execution_id}"
        )

        bindings: Bindings = []
        write_later: List[Tuple[str, Any]] = []
        inputs: List[InputExecutionValue] = []

        for name, out_type, data in self.resolve_args():
            slot = self._zygote.slot(name)
            if isinstance(data, self.__EntryId):
                entry_id = data.entry_id
                hash_ = None
            else:
                entry_id = self._entry_id_generator.generate(slot.name)
                hash_ = self._hasher.hash(data)
                write_later.append((entry_id, data))

            channel = self._channel_manager.channel(entry_id, DataSchema.generate_schema(out_type))
            bindings.append(Binding(slot, channel))
            inputs.append(InputExecutionValue(name, entry_id, hash_))

        for return_slot, val in zip(self.zygote.return_slots, self.return_values()):
            bindings.append(
            Binding(
                return_slot,
                self._channel_manager.channel(
                    val.entry_id, DataSchema.generate_schema(val.type)
                ),
            )
        )

        if self._cache_policy.restore():
            executions = self._servant.resolve_executions(
                self.signature.func.name, self._snapshot_id, inputs
            )
            if len(executions) >= 1:
                values = {
                    v.name: v.entry_id for v in executions[0].outputs
                }
                for num, return_value in enumerate(self.return_values()):
                    return_value.change_entry_id(values[os.path.join("return", str(num))])
                return

        description = (
            self._build_description(inputs) if self._cache_policy.save() else None
        )

        self._zygote.execution_description = description

        execution = self._servant.run(execution_id, self._zygote, bindings)

        self.dump_arguments(write_later)

        func = self.signature.func

        result = execution.wait_for()
        rc_ = result.returncode
        if rc_ == 0:
            self._log.info(
                f"Executed task {execution.id()[:4]} for func {self.signature.func.name} with rc {rc_}",
            )
            return

        message = self._execution_exception_message(execution, func, rc_)
        self._log.error(f"Execution exception with message: {message}")
        raise LzyExecutionException(message)

    def _build_description(
        self, inputs: Iterable[InputExecutionValue]
    ) -> ExecutionDescription:
        return ExecutionDescription(
            self.signature.func.name,
            self._snapshot_id,
            inputs,
            tuple(
                ExecutionValue(os.path.join("return", str(num)), val.entry_id)
                for num, val in enumerate(self.return_values())
            ),
        )

    def materialize(self) -> Tuple:
        name = self.signature.func.name
        self._log.info("Materializing function %s", name)
        if self._materialized and self._materialization is not None:
            return self._materialization
        if self._deployed:
            self._materialized = True
            mat = self.signature.exec()
            if len(self.return_values()) == 1:
                self._materialization = tuple((mat,))
            else:
                self._materialization = mat
            return self._materialization
        else:
            self.execute()
            materialization: List[Any] = []
            for val in self.return_values():
                output_data_scheme = DataSchema.generate_schema(val.type)
                path = self._channel_manager.in_slot(
                    val.entry_id, output_data_scheme
                )
                try:
                    with path.open("rb") as handle:
                        # Wait for slot to become open
                        while handle.read(1) is None:
                            time.sleep(0)  # Thread.yield
                            if not path.exists():
                                raise LzyExecutionException("Cannot read from slot")
                        handle.seek(0)
                        materialization.append(
                            self._file_serializer.deserialize_from_file(
                                handle, val.type
                            )
                        )
                except Exception as e:
                    self._log.error(e)
                    raise LzyExecutionException(
                        "Materialization failed: cannot read data from return slot"
                    )
                finally:
                    self._channel_manager.destroy(val.entry_id)
            self._materialized = True
            self._log.info("Materializing function %s done", name)
            self._materialization = tuple(materialization)
            return self._materialization

    def execute(self):
        if self._executed:
            return
        self.execution_logic()
        self._executed = True

    # pylint: disable=too-many-arguments
    @staticmethod
    def restore(
        servant: ServantClient,
        materialized: bool,
        materialization: Any,
        call_s: CallSignature[Tuple],
        provisioning: Provisioning,
        env: Env,
        snapshot_id: str,
        mem_serializer: MemBytesSerializer,
        file_serializer: FileSerializer,
        hasher: Hasher,
    ):
        op_ = LzyRemoteOp(
            servant,
            call_s,
            snapshot_id,
            UUIDEntryIdGenerator(snapshot_id),
            mem_serializer,
            file_serializer,
            hasher,
            provisioning,
            env.base_env,
            env.aux_env,
            deployed=False,
        )
        op_._materialized = materialized  # pylint: disable=protected-access
        op_._materialization = materialization  # pylint: disable=protected-access
        return op_

    @staticmethod
    def reducer(op_: "LzyRemoteOp") -> Any:
        return LzyRemoteOp.restore, (
            # pylint: disable=protected-access
            op_._servant,
            op_.is_materialized(),
            op_._materialization,
            op_.signature,
            op_.zygote.provisioning,
            op_.zygote.env,
            op_._snapshot_id,
            op_._mem_serializer,
            op_._file_serializer,
            op_._hasher,
        )

    def _destroy_binding(self, binding: Binding) -> None:
        self._servant.destroy_channel(binding.channel)


copyreg.dispatch_table[LzyRemoteOp] = LzyRemoteOp.reducer  # type: ignore
