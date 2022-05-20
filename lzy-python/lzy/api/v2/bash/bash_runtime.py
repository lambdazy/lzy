import dataclasses
import os
import tempfile
import uuid
import zipfile
from typing import Iterable, Tuple, Union, Any, Callable, Dict

from lzy.storage.storage_client import StorageClient
from lzy.api.v2.api import Provisioning, Gpu
from lzy.api.v2.api.graph import Graph
from lzy.api.v2.api.lzy_call import LzyCall
from lzy.api.v2.api.runtime.runtime import Runtime
from lzy.api.v2.bash.task_spec import TaskSpec
from lzy.api.v2.servant.bash_servant_client import BashServantClient
from lzy.api.v2.servant.model.env import Env, BaseEnv, PyEnv
from lzy.api.v2.servant.model.signatures import FuncSignature
from lzy.api.v2.servant.model.zygote_python_func import ZygotePythonFunc
from lzy.api.v2.servant.servant_client import ServantClient
from lzy.api.v2.api.snapshot.snapshot import Snapshot
from lzy.serialization.serializer import Serializer
from lzy.api.v2.servant.channel_manager import ChannelManager
from lzy.api.v2.servant.model.channel import Bindings, Binding
from lzy.api.v2.utils import is_lazy_proxy, zipdir


class BashRuntime(Runtime):
    def __init__(self, lzy_mount: str, serializer: Serializer, channel_manager: ChannelManager,
                 servant_client: ServantClient, storage_client: StorageClient):
        self._lzy_mount = lzy_mount
        self._serializer = serializer
        self._channel_manager = channel_manager
        self._servant_client: BashServantClient = BashServantClient().instance(lzy_mount)
        self._storage_client = storage_client
        self._bucket = servant_client.get_bucket()

    @dataclasses.dataclass
    class __EntryId:
        entry_id: str

    def _resolve_args(self, call: LzyCall) -> Iterable[Tuple[str, Union[Any, __EntryId]]]:
        for name, arg in call.named_arguments():
            if is_lazy_proxy(arg) and hasattr(arg, "_origin"):
                yield name, self.__EntryId(arg.lzy_call.entry_id)
                continue
            yield name, arg

    def _env(self, call: LzyCall) -> Env:
        base_env = call.op.env.base_env
        aux_env = call.op.env.aux_env
        if aux_env is None:
            return Env(base_env=BaseEnv(base_env.base_docker_image))

        local_modules_uploaded = []
        for local_module in aux_env.local_modules_paths:
            with tempfile.NamedTemporaryFile("rb") as archive:
                if not os.path.isdir(local_module):
                    with zipfile.ZipFile(archive.name, "w") as z:
                        z.write(local_module, os.path.basename(local_module))
                else:
                    with zipfile.ZipFile(archive.name, "w") as z:
                        zipdir(local_module, z)
                archive.seek(0)
                key = "local_modules/" + os.path.basename(local_module) + "/" \
                      + fileobj_hash(archive.file)  # type: ignore
                archive.seek(0)
                if not self._storage_client.blob_exists(self._bucket, key):
                    self._storage_client.write(self._bucket, key, archive)  # type: ignore
                uri = self._storage_client.generate_uri(self._bucket, key)
            local_modules_uploaded.append((os.path.basename(local_module), uri))

        py_env = PyEnv(aux_env.name, aux_env.conda_yaml, local_modules_uploaded)
        return Env(aux_env=py_env, base_env=BaseEnv(base_env.base_docker_image))

    def _task_spec(self, call: LzyCall, snapshot_id: str):
        call_id = call.id
        operation_name = call.operation_name
        if call.op.provisioning.gpu is not None and call.op.provisioning.gpu.is_any:
            provisioning = Provisioning(Gpu.any())
        else:
            provisioning = Provisioning()

        zygote = ZygotePythonFunc(
            self._serializer,
            FuncSignature(call.op.callable, call.op.input_types, call.op.output_type, call.op.arg_names,
                          call.op.kwarg_names),
            self._env(call),
            provisioning
        )

        args = self._resolve_args(call)
        bindings: Bindings = []
        write_later: Dict[str, Any] = {}

        for name, data in args:
            slot = zygote.slot(name)
            if isinstance(data, self.__EntryId):
                channel = self._channel_manager.channel(snapshot_id=snapshot_id, entry_id=data.entry_id)
                bindings.append(Binding(slot, channel))
            else:
                entry_id = "/".join([call_id, slot.name, str(uuid.uuid1())])
                channel = self._channel_manager.channel(snapshot_id=snapshot_id, entry_id=entry_id)
                bindings.append(Binding(slot, channel))
                write_later[entry_id] = data

        bindings.append(Binding(zygote.return_slot, self._channel_manager.channel(snapshot_id, call.entry_id)))

        dependent_calls = []
        for name, arg in call.named_arguments():
            if is_lazy_proxy(arg) and not hasattr(arg, "_origin"):
                dependent_calls.append(arg.id)

        return TaskSpec(call_id, operation_name, zygote, bindings, dependent_calls), write_later

    def exec(self, graph: Graph, snapshot: Snapshot, progress: Callable[[], None]):
        graph_description = []
        write_later: Dict[str, Any] = {}
        for call in graph.calls():
            task_spec, write_later_slots = self._task_spec(call, snapshot.id())
            graph_description.append(task_spec)
            write_later.update(write_later_slots)
        # TODO: write to slots, send tasks for execution

    def destroy(self) -> None:
        self._channel_manager.destroy_all()
