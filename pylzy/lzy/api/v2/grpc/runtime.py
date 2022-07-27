import os
import tempfile
import uuid
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

from lzy.api.v2.api import LzyCall
from lzy.api.v2.api.runtime.runtime import Runtime
from lzy.api.v2.api.snapshot.snapshot import Snapshot
from lzy.api.v2.grpc.graph_executor_client import GraphExecutorClient

from lzy.api.v2.utils import unwrap
from lzy.api.v2.proxy_adapter import is_lzy_proxy, materialized
from lzy.serialization.serializer import Serializer
from lzy.storage.credentials import StorageCredentials
from lzy.storage.storage_client import StorageClient, from_credentials

from lzy.api.v2.servant.model.slot import file_slot_t


# model
from ai.lzy.v1.channel_pb2 import Binding
from ai.lzy.v1.task_pb2 import TaskSpec
from ai.lzy.v1.zygote_pb2 import Slot, _SLOT_DIRECTION
from ai.lzy.v1.zygote_pb2 import EnvSpec, AuxEnv, BaseEnv


def _generate_channel_name(call_id: str):
    return f"local://{call_id}/return"


def _get_slot_path(slot: Slot) -> Path:
    # TODO: should mount point be passed from user?
    mount = Path(os.getenv("LZY_MOUNT", default="/tmp/lzy"))
    return mount.joinpath(slot.name.lstrip(os.path.sep))


def _get_or_generate_call_ids(call) -> Dict[str, str]:
    arg_name_to_call_id = {}
    for name, arg in call.named_arguments():
        if is_lzy_proxy(arg):
            arg_name_to_call_id[name] = arg.lzy_call.id
        else:
            arg_name_to_call_id[name] = str(uuid.uuid4())
    return arg_name_to_call_id


class GrpcRuntime(Runtime):
    def __init__(
        self,
        storage_client: StorageClient,
        bucket: str,
        graph_executor_client: GraphExecutorClient,
    ):
        self._storage_client = storage_client
        self._bucket = bucket
        self._graph_executor_client = graph_executor_client

    @classmethod
    def from_credentials(
        cls, credentials: StorageCredentials, bucket: str
    ) -> "GrpcRuntime":
        return cls(from_credentials(credentials), bucket)

    def _load_arg(self, entry_id: str, data: Any, serializer: Serializer):
        with tempfile.NamedTemporaryFile("wb", delete=True) as write_file:
            serializer.serialize_to_file(data, write_file)
            write_file.flush()
            with open(write_file.name, "rb") as read_file:
                read_file.seek(0)
                uri = self._storage_client.write(self._bucket, entry_id, read_file)
                # TODO: make a call to snapshot component to store entry_id and uri

    def _load_args(self, graph: List[LzyCall], serializer: Serializer):
        for call in graph.calls():
            for name, arg in call.named_arguments():
                if not is_lzy_proxy(arg):
                    entry_id = str(uuid.uuid4())
                    self._load_arg(entry_id, arg, serializer)

    def _env(self, call: LzyCall) -> EnvSpec:
        base_env: BaseEnv = unwrap(call.op.env.base_env)
        env = EnvSpec(baseEnv=base_env)
        aux_env: Optional[AuxEnv] = unwrap(call.op.env.aux_env)
        if aux_env is not None:
            env.aux_env = aux_env
        return env

        # local_modules_uploaded = []
        # for local_module in aux_env.local_modules_paths:
        #     with tempfile.NamedTemporaryFile("rb") as archive:
        #         if not os.path.isdir(local_module):
        #             with zipfile.ZipFile(archive.name, "w") as z:
        #                 z.write(local_module, os.path.basename(local_module))
        #         else:
        #             with zipfile.ZipFile(archive.name, "w") as z:
        #                 zipdir(local_module, z)
        #         archive.seek(0)
        #         key = (
        #             "local_modules/"
        #             + os.path.basename(local_module)
        #             + "/"
        #             + fileobj_hash(archive.file)  # type: ignore
        #         )  # type: ignore
        #         archive.seek(0)
        #         if not self._storage_client.blob_exists(self._bucket, key):
        #             self._storage_client.write(self._bucket, key, archive)  # type: ignore
        #         uri = self._storage_client.generate_uri(self._bucket, key)
        #     local_modules_uploaded.append((os.path.basename(local_module), uri))

        # py_env = PyEnv(aux_env.name, aux_env.conda_yaml, local_modules_uploaded)
        # if base_env is None:
        #     return Env(aux_env=py_env)
        # return Env(aux_env=py_env, base_env=BaseEnv(base_env.base_docker_image))

    def _dump_arguments(
        self,
        call: LzyCall,
        arg_name_to_call_id: Dict[str, str],
        snapshot_id: str,
        serializer: Serializer,
    ):
        for name, arg in call.named_arguments():
            if is_lzy_proxy(arg):
                continu
            call_id = arg_name_to_call_id[name]
            slot = file_slot_t(
                os.path.sep.join(("tasks", "snapshot", snapshot_id, call_id)),
                _SLOT_DIRECTION.OUTPUT,
            )
            self._channel_manager.touch(
                slot, self._channel_manager.snapshot_channel(snapshot_id, call_id)
            )
            path = _get_slot_path(slot)
            # with path.open('wb') as handle:
            #     serializer.serialize_to_file(arg, handle)
            #     handle.flush()
            #     os.fsync(handle.fileno())
            #

    #  def _zygote(self, call: LzyCall, serializer: Serializer) -> Zygote:
    #      if call.op.provisioning.gpu is not None and call.op.provisioning.gpu.is_any:
    #          provisioning = Provisioning(Gpu.any())
    #      else:
    #          provisioning = Provisioning()

    def _bindings(
        self,
        call: LzyCall,
        snapshot_id: str,
        arg_name_to_call_id: Dict[str, str],
    ):
        bindings: List[Binding] = []
        for name, arg in call.named_arguments():
            # slot: Slot = zygote.slot(name)
            if is_lzy_proxy(arg) and not materialized(arg):
                call_id = arg.lzy_call.id
            else:
                call_id = arg_name_to_call_id[name]
            channel = self._channel_manager.snapshot_channel(
                snapshot_id, _generate_channel_name(call_id)
            )
            # bindings.append(Binding(slot, channel))
        return bindings

    def _task_spec(
        self, call: LzyCall, snapshot_id: str, serializer: Serializer
    ) -> TaskSpec:
        zygote = call.zygote
        arg_name_to_call_id: Dict[str, str] = _get_or_generate_call_ids(call)
        bindings: List[Binding] = self._bindings(
            call, zygote, snapshot_id, arg_name_to_call_id
        )
        self._dump_arguments(call, arg_name_to_call_id, snapshot_id, serializer)
        return TaskSpec(call.id, zygote, bindings)

    def exec(
        self, graph: List[LzyCall], snapshot: Snapshot, progress: Callable[[], None]
    ) -> None:
        raise NotImplementedError()
        # TODO[ottergottaott]: write this part up
        # zygote = python_func_zygote(
        #     active_wflow.owner._serializer,
        #     signature.func,
        #     active_wflow._env_provider.for_op(caller_globals),
        #     provisioning,
        # )

        try:
            task_specs: List[TaskSpec] = []
            for call in graph.calls():
                task_specs.append(
                    self._task_spec(call, snapshot.id(), snapshot.serializer())
                )
            # here workflow_id == snapshot_id is assumed, but need to make it separate later
            self._graph_executor_client.execute(snapshot.id(), task_specs)
        finally:
            self._channel_manager.destroy()

    def destroy(self) -> None:
        self._channel_manager.destroy()
