import copyreg
import dataclasses
import logging
import os
import time
import uuid
from abc import abstractmethod, ABC
from typing import Optional, Any, TypeVar, Generic, Tuple, Iterable, Union, List

from pure_protobuf.dataclasses_ import load, Message  # type: ignore

from lzy.api.v1.servant.channel_manager import ChannelManager
from lzy.api.v1.servant.model.channel import Bindings, Binding
from lzy.api.v1.servant.model.env import PyEnv, Env
from lzy.api.v1.servant.model.execution import Execution, InputExecutionValue, ExecutionDescription, ExecutionValue
from lzy.api.v1.servant.model.return_codes import ReturnCode
from lzy.api.v1.servant.model.zygote import Provisioning, Zygote
from lzy.api.v1.servant.model.zygote_python_func import ZygotePythonFunc
from lzy.api.v1.servant.servant_client import ServantClient
from lzy.api.v1.utils import is_lazy_proxy, LzyExecutionException
from lzy.api.v1.cache_policy import CachePolicy
from lzy.api.v1.signatures import CallSignature, FuncSignature
from lzy.api.v1.whiteboard.model import EntryIdGenerator, UUIDEntryIdGenerator
from lzy.serialization.hasher import Hasher
from lzy.serialization.serializer import MemBytesSerializer, FileSerializer

T = TypeVar("T")  # pylint: disable=invalid-name


class LzyOp(Generic[T], ABC):
    def __init__(self, signature: CallSignature[T], return_entry_id: str):
        super().__init__()
        self._sign: CallSignature[T] = signature
        self._materialized: bool = False
        self._executed: bool = False
        self._materialization: Optional[T] = None
        self._log: logging.Logger = logging.getLogger(str(self.__class__))
        self._return_entry_id = return_entry_id

    @property
    def signature(self) -> CallSignature[T]:
        return self._sign

    def is_materialized(self) -> bool:
        return self._materialized

    @abstractmethod
    def materialize(self) -> T:
        pass

    @abstractmethod
    def execute(self):
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
            self.execute()
            self._log.info("Materializing function %s done", name)
        else:
            self._log.info("Function %s has been already materialized", name)
        return self._materialization

    def execute(self):
        if self._executed:
            return
        self._materialization: T = self.signature.exec()
        self._materialized = True


class LzyRemoteOp(LzyOp, Generic[T]):
    def __init__(
            self,
            servant: ServantClient,
            signature: CallSignature[T],
            snapshot_id: str,
            entry_id_generator: EntryIdGenerator,
            mem_serializer: MemBytesSerializer,
            file_serializer: FileSerializer,
            hasher: Hasher,
            provisioning: Optional[Provisioning] = None,
            env: Optional[PyEnv] = None,
            deployed: bool = False,
            channel_manager: Optional[ChannelManager] = None,
            cache_policy: CachePolicy = CachePolicy.IGNORE
    ):
        if (not provisioning or not env) and not deployed:
            raise ValueError("Non-deployed ops must have provisioning and env")

        self._hasher = hasher
        self._mem_serializer = mem_serializer
        self._file_serializer = file_serializer
        self._deployed = deployed
        self._servant = servant
        self._env = env
        self._snapshot_id: str = snapshot_id
        self._cache_policy = cache_policy

        if not deployed and not channel_manager:
            raise ValueError("ChannelManager not provided")

        self._channel_manager: ChannelManager = channel_manager  # type: ignore

        for input_type in signature.func.input_types.values():
            if issubclass(input_type, Message):
                setattr(input_type, 'LZY_MESSAGE', 'LZY_WB_MESSAGE')

        output_type = signature.func.output_type
        if issubclass(output_type, Message):
            setattr(output_type, 'LZY_MESSAGE', 'LZY_WB_MESSAGE')

        self._zygote = ZygotePythonFunc(
            mem_serializer,
            signature.func,
            Env(aux_env=env),
            provisioning
        )

        self._entry_id_generator = entry_id_generator

        super().__init__(signature, entry_id_generator.generate(self._zygote.return_slot.name))

    @property
    def zygote(self) -> Zygote:
        return self._zygote

    def dump_arguments(self, args: Iterable[Tuple[str, Any]]):
        for entry_id, obj in args:
            path = self._channel_manager.out_slot(entry_id)
            with path.open('wb') as handle:
                self._file_serializer.serialize_to_file(obj, handle)
                handle.flush()
                os.fsync(handle.fileno())

    @classmethod
    def _execution_exception_message(
            cls,
            execution: Execution,
            func: FuncSignature[Any], return_code: int) -> str:

        if return_code == ReturnCode.ENVIRONMENT_INSTALLATION_ERROR.value:
            message = "Failed to install environment on remote machine"
        elif return_code == ReturnCode.EXECUTION_ERROR.value:
            message = "Lzy error"
        else:
            message = "Execution error"
        return LzyRemoteOp._exception(execution, func, return_code, message)

    @classmethod
    def _exception(cls, execution: Execution, func: FuncSignature[Any],
                   returncode: int, message: str) -> str:
        return (
            f"Task {execution.id()[:4]} failed in func {func.name}"
            f"with rc {returncode} and message: {message}"
        )

    @dataclasses.dataclass
    class __EntryId:
        entry_id: str

    def resolve_args(self) -> Iterable[Tuple[str, Union[Any, __EntryId]]]:
        for name, arg in self.signature.named_arguments():
            if is_lazy_proxy(arg):
                # noinspection PyProtectedMember
                op: LzyOp = arg._op
                op.execute()
                yield name, self.__EntryId(op.return_entry_id())
                continue
            yield name, arg

    def execution_logic(self):
        execution_id = str(uuid.uuid4())
        self._log.info(f"Running zygote {self._zygote.name}, execution id {execution_id}")

        args = self.resolve_args()
        bindings: Bindings = []
        write_later: List[Tuple[str, Any]] = []
        inputs: List[InputExecutionValue] = []

        for name, data in args:
            slot = self._zygote.slot(name)
            if isinstance(data, self.__EntryId):
                channel = self._channel_manager.channel(entry_id=data.entry_id)
                inputs.append(InputExecutionValue(name, data.entry_id, None))
                bindings.append(Binding(slot, channel))
            else:
                entry_id = self._entry_id_generator.generate(slot.name)
                channel = self._channel_manager.channel(entry_id)
                bindings.append(Binding(slot, channel))
                write_later.append((entry_id, data))
                inputs.append(InputExecutionValue(name, entry_id, self._hasher.hash(data)))

        bindings.append(Binding(self.zygote.return_slot, self._channel_manager.channel(self.return_entry_id())))

        if self._cache_policy.restore():
            executions = self._servant.resolve_executions(self.signature.func.name, self._snapshot_id, inputs)
            if len(executions) >= 1:
                return_value = filter(lambda x: x.name == "return", executions[0].outputs).__next__()
                self._return_entry_id = return_value.entry_id
                return

        description = self._build_description(inputs) if self._cache_policy.save() else None

        self._zygote.execution_description = description

        execution = self._servant.run(
            execution_id, self._zygote,
            bindings
        )

        self.dump_arguments(write_later)

        func = self.signature.func

        result = execution.wait_for()
        rc_ = result.returncode
        if rc_ == 0:
            self._log.info("Executed task %s for func %s with rc %s",
                           execution.id()[:4], self.signature.func.name, rc_, )
            return

        message = self._execution_exception_message(execution, func, rc_)
        self._log.error(f"Execution exception with message: {message}")
        raise LzyExecutionException(message)

    def _build_description(self, inputs: Iterable[InputExecutionValue]) -> ExecutionDescription:
        return ExecutionDescription(
            self.signature.func.name,
            self._snapshot_id,
            inputs,
            (ExecutionValue("return", self.return_entry_id()),)
        )

    def materialize(self) -> Any:
        name = self.signature.func.name
        self._log.info("Materializing function %s", name)
        if not self._materialized:
            if self._deployed:
                self._materialization = self.signature.exec()
            else:
                self.execute()
                path = self._channel_manager.in_slot(self.return_entry_id())
                try:
                    with path.open("rb") as handle:
                        # Wait for slot to become open
                        while handle.read(1) is None:
                            time.sleep(0)  # Thread.yield
                            if not path.exists():
                                raise LzyExecutionException("Cannot read from slot")
                        handle.seek(0)
                        self._materialization = self._file_serializer.deserialize_from_file(handle,
                                                                                            self.signature.func.output_type)
                        self._materialized = True
                        self._log.info("Materializing function %s done", name)
                except Exception as e:
                    self._log.error(e)
                    raise LzyExecutionException("Materialization failed: cannot read data from return slot")
                finally:
                    self._channel_manager.destroy(self.return_entry_id())
        else:
            # noinspection PyTypeChecker
            self._log.info("Function %s has been already materialized", name)
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
            call_s: CallSignature[T],
            provisioning: Provisioning,
            env: PyEnv,
            snapshot_id: str,
            mem_serializer: MemBytesSerializer,
            file_serializer: FileSerializer,
            hasher: Hasher
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
            env,
            deployed=False
        )
        op_._materialized = materialized  # pylint: disable=protected-access
        op_._materialization = materialization  # pylint: disable=protected-access
        return op_

    @staticmethod
    def reducer(op_: "LzyRemoteOp") -> Any:
        return LzyRemoteOp.restore, (
            # pylint: disable=protected-access
            op_._servant, op_.is_materialized(), op_._materialization,
            op_.signature,
            op_.zygote.provisioning,
            op_.zygote.env,
            op_._snapshot_id,
            op_._mem_serializer,
            op_._file_serializer,
            op_._hasher
        )

    def _destroy_binding(self, binding: Binding) -> None:
        self._servant.destroy_channel(binding.channel)


copyreg.dispatch_table[LzyRemoteOp] = LzyRemoteOp.reducer  # type: ignore
