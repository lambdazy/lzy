import dataclasses
import logging
import os
import tempfile
from yaml import safe_load
from abc import abstractmethod, ABC
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Tuple, Callable, Type, Any, TypeVar, Iterable, Optional

from lzy.api.v1.buses import Bus
from lzy.api.v1.cache_policy import CachePolicy
from lzy.api.v1.lazy_op import LzyOp
from lzy.pkg_info import all_installed_packages, create_yaml, select_modules
import zipfile

from lzy.api.v1.servant.model.encoding import ENCODING as encoding
from lzy.api.v1.utils import zipdir, fileobj_hash
from lzy.serialization.hasher import DelegatingHasher, Hasher
from lzy.serialization.serializer import FileSerializerImpl, MemBytesSerializerImpl, MemBytesSerializer, FileSerializer
from lzy.storage.storage_client import StorageClient, from_credentials
from lzy.api.v1.whiteboard import wrap_whiteboard, wrap_whiteboard_for_read, check_whiteboard
from lzy.api.v1.whiteboard.model import (
    InMemSnapshotApi,
    InMemWhiteboardApi,
    SnapshotApi,
    WhiteboardApi,
    WhiteboardList,
    WhiteboardDescription,
    WhiteboardFieldStatus, UUIDEntryIdGenerator
)
from lzy.api.v1.servant.model.env import PyEnv
from lzy.api.v1.servant.bash_servant_client import BashServantClient
from lzy.api.v1.servant.channel_manager import ServantChannelManager, LocalChannelManager, ChannelManager
from lzy.api.v1.servant.servant_client import ServantClient, CredentialsTypes, ServantClientMock
from lzy.api.v1.servant.whiteboard_bash_api import SnapshotBashApi, WhiteboardBashApi

T = TypeVar("T")  # pylint: disable=invalid-name
BusList = List[Tuple[Callable, Bus]]


class LzyEnvBase(ABC):
    # pylint: disable=too-many-arguments
    def __init__(self,
                 whiteboard_api: WhiteboardApi,
                 snapshot_api: SnapshotApi,
                 ):
        self._whiteboard_api = whiteboard_api
        self._snapshot_api = snapshot_api
        self._log = logging.getLogger(str(self.__class__))

    def whiteboard(self, wid: str, typ: Type[Any]) -> Any:
        check_whiteboard(typ)
        wb_ = self._whiteboard_api.get(wid)
        return self._build_whiteboard(wb_, typ)

    def _build_whiteboard(self, wb_: WhiteboardDescription, typ: Type[Any]) -> Any:
        check_whiteboard(typ)
        # noinspection PyDataclass
        field_types = {field.name: field.type for field in dataclasses.fields(typ)}
        whiteboard_dict: Dict[str, Any] = {}
        for field in wb_.fields:
            if field.field_name in field_types:
                if field.status is WhiteboardFieldStatus.FINISHED:
                    whiteboard_dict[field.field_name] = self._whiteboard_api \
                        .resolve(field.storage_uri, field_types[field.field_name])
        # noinspection PyArgumentList
        instance = typ(**whiteboard_dict)
        wrap_whiteboard_for_read(instance, wb_)
        return instance

    def _whiteboards(self, namespace: str, tags: List[str], typ: Type[T], from_date: datetime = None,
                     to_date: datetime = None) -> List[T]:
        check_whiteboard(typ)
        wb_list = self._whiteboard_api.list(namespace, tags, from_date, to_date)
        self._log.info(f"Received whiteboards list in namespace {namespace} and tags {tags} "
                       f"within dates {from_date} - {to_date}")
        result = []
        for wb_ in wb_list:
            try:
                wb = self._build_whiteboard(wb_, typ)
                result.append(wb)
            except TypeError:
                self._log.warning(f"Could not create whiteboard with type {typ}")
        return result

    def whiteboards(self, typs: List[Type[T]], from_date: datetime = None, to_date: datetime = None) -> WhiteboardList:
        whiteboard_dict = {}
        for typ in typs:
            check_whiteboard(typ)
            whiteboard_dict[typ] = self._whiteboards(typ.LZY_WB_NAMESPACE, typ.LZY_WB_TAGS, typ, from_date, to_date)  # type: ignore
        self._log.info(f"Whiteboard dict is {whiteboard_dict}")
        list_of_wb_lists = list(whiteboard_dict.values())
        return WhiteboardList([wb for wbs_list in list_of_wb_lists for wb in wbs_list])


class WhiteboardExecutionContext:
    def __init__(
            self, whiteboard_api: WhiteboardApi, snapshot_api: SnapshotApi,
            whiteboard: Any, workflow_name: str, from_last_snapshot: bool = False
    ):
        self._snapshot_id: Optional[str] = None
        self._whiteboard_id: Optional[str] = None
        self.whiteboard_api = whiteboard_api
        self.snapshot_api = snapshot_api
        self.whiteboard = whiteboard
        self.workflow_name = workflow_name
        self.from_last_snapshot = from_last_snapshot

    @property
    def snapshot_id(self) -> str:
        if self._snapshot_id is not None:
            return self._snapshot_id
        if not self.from_last_snapshot:
            self._snapshot_id = self.snapshot_api.create(self.workflow_name).snapshot_id
        else:
            snapshot = self.snapshot_api.last(self.workflow_name)
            if not snapshot:
                self._snapshot_id = self.snapshot_api.create(self.workflow_name).snapshot_id
            else:
                self._snapshot_id = snapshot.snapshot_id
        return self._snapshot_id

    @property
    def whiteboard_id(self) -> Optional[str]:
        if self._whiteboard_id is not None:
            return self._whiteboard_id

        if self.whiteboard_api is None or self.whiteboard is None:
            return None

        snapshot_id = self.snapshot_id

        fields = dataclasses.fields(self.whiteboard)
        self._whiteboard_id = self.whiteboard_api.create(
            [field.name for field in fields], snapshot_id,
            self.whiteboard.LZY_WB_NAMESPACE, self.whiteboard.LZY_WB_TAGS
        ).id
        return self._whiteboard_id


class LzyLocalEnv(LzyEnvBase):
    def __init__(self):
        super().__init__(
            whiteboard_api=InMemWhiteboardApi(),
            snapshot_api=InMemSnapshotApi(),
        )
        self._file_serializer = FileSerializerImpl()

    def workflow(
            self,
            name: str,
            eager: bool = False,
            whiteboard: Any = None,
            buses: Optional[BusList] = None
    ):
        return LzyLocalWorkflow(
            name=name,
            file_serializer=self._file_serializer,
            eager=eager,
            whiteboard=whiteboard,
            buses=buses
        )


class LzyRemoteEnv(LzyEnvBase):
    def __init__(
            self,
            lzy_mount: str = os.getenv("LZY_MOUNT", default="/tmp/lzy"),
    ):
        self._servant_client: BashServantClient = BashServantClient.instance(lzy_mount)
        self._lzy_mount = lzy_mount
        self._mem_serializer = MemBytesSerializerImpl()
        self._file_serializer = FileSerializerImpl()
        self._hasher = DelegatingHasher(self._file_serializer)
        super().__init__(
            whiteboard_api=WhiteboardBashApi(lzy_mount, self._servant_client, self._file_serializer),
            snapshot_api=SnapshotBashApi(lzy_mount),
        )

    def workflow(
            self,
            name: str,
            cache_policy: CachePolicy = CachePolicy.IGNORE,
            eager: bool = False,
            whiteboard: Any = None,
            buses: Optional[BusList] = None,
            conda_yaml_path: Optional[Path] = None,
            local_module_paths: Optional[List[str]] = None,
    ):
        return LzyRemoteWorkflow(
            name=name,
            whiteboard_api=self._whiteboard_api,
            snapshot_api=self._snapshot_api,
            lzy_mount=self._lzy_mount,
            conda_yaml_path=conda_yaml_path,
            local_module_paths=local_module_paths,
            cache_policy=cache_policy,
            eager=eager,
            whiteboard=whiteboard,
            buses=buses,
            mem_serializer=self._mem_serializer,
            file_serializer=self._file_serializer,
            hasher=self._hasher
        )


class LzyWorkflowBase(ABC):
    instances: List["LzyWorkflowBase"] = []

    def __init__(
            self,
            whiteboard_api: WhiteboardApi,
            snapshot_api: SnapshotApi,
            servant_client: ServantClient,
            name: str,
            eager: bool = False,
            whiteboard: Any = None,
            buses: Optional[BusList] = None,
            cache_policy: CachePolicy = CachePolicy.IGNORE
    ):
        self._execution_context = WhiteboardExecutionContext(
            whiteboard_api, snapshot_api, whiteboard, name, cache_policy.from_last_snapshot()
        )
        self._ops: List[LzyOp] = []
        self._buses = list(buses or [])
        self._eager = eager
        self._name = name
        self._log = logging.getLogger(str(self.__class__))
        self._servant_client = servant_client
        self._cache_policy = cache_policy

    def register_op(self, lzy_op: LzyOp) -> None:
        self._ops.append(lzy_op)
        if self._eager:
            lzy_op.materialize()

    def whiteboard_id(self) -> Optional[str]:
        return self._execution_context.whiteboard_id

    def snapshot_id(self) -> str:
        return self._execution_context.snapshot_id

    def registered_ops(self) -> Iterable[LzyOp]:
        # if self.get_active() is None:
        #    raise ValueError("Fetching ops on a non-entered environment")
        return list(self._ops)

    def run(self) -> None:
        assert self.get_active() is not None
        for wrapper in self._ops:
            wrapper.execute()

    @property
    def cache_policy(self) -> CachePolicy:
        return self._cache_policy

    @abstractmethod
    def activate(self):
        pass

    @abstractmethod
    def deactivate(self):
        pass

    @classmethod
    def get_active(cls) -> "LzyWorkflowBase":
        assert len(cls.instances) > 0, "There is not active LzyWorkflow"
        return cls.instances[-1]

    def __enter__(self) -> "LzyWorkflowBase":
        self.activate()
        type(self).instances.append(self)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        try:
            if not exc_val:
                self.run()
            context = self._execution_context
            whiteboard = context.whiteboard
            if whiteboard is not None:
                fields = dataclasses.fields(whiteboard)
                for field in fields:
                    if field.name not in whiteboard.__lzy_fields_assigned__:
                        value = getattr(whiteboard, field.name)
                        setattr(whiteboard, field.name, value)
            # pylint: disable=protected-access
            # noinspection PyProtectedMember
            if context._snapshot_id is not None:
                # noinspection PyProtectedMember
                context.snapshot_api.finalize(context._snapshot_id)
        finally:
            self.deactivate()
            type(self).instances.pop()


class LzyRemoteWorkflow(LzyWorkflowBase):
    def activate(self):
        pass

    def deactivate(self):
        self.channel_manager().destroy_all()

    def __init__(
            self,
            name: str,
            whiteboard_api: WhiteboardApi,
            snapshot_api: SnapshotApi,
            mem_serializer: MemBytesSerializer,
            file_serializer: FileSerializer,
            hasher: Hasher,
            lzy_mount: str = os.getenv("LZY_MOUNT", default="/tmp/lzy"),
            conda_yaml_path: Optional[Path] = None,
            local_module_paths: Optional[List[str]] = None,
            cache_policy: CachePolicy = CachePolicy.IGNORE,
            eager: bool = False,
            whiteboard: Any = None,
            buses: Optional[BusList] = None,

    ):
        self._mem_serializer = mem_serializer
        self._file_serializer = file_serializer
        self._hasher = hasher
        self._yaml = conda_yaml_path
        self._restart_policy = cache_policy
        self._servant_client: BashServantClient = BashServantClient.instance(lzy_mount)
        self._py_env: Optional[PyEnv] = None

        bucket = self._servant_client.get_bucket()
        self._bucket = bucket

        credentials = self._servant_client.get_credentials(CredentialsTypes.S3, bucket)
        self._storage_client: StorageClient = from_credentials(credentials)
        self._local_module_paths = local_module_paths

        super().__init__(
            name=name,
            whiteboard_api=whiteboard_api,
            snapshot_api=snapshot_api,
            servant_client=self._servant_client,
            eager=eager,
            whiteboard=whiteboard,
            buses=buses,
            cache_policy=cache_policy
        )
        snapshot_id = self.snapshot_id()
        if snapshot_id is None:
            raise ValueError("Cannot get snapshot id")
        else:
            self._channel_manager = ServantChannelManager(snapshot_id, self._servant_client)

        if self._execution_context.whiteboard is not None:
            check_whiteboard(whiteboard)
            wrap_whiteboard(
                self._execution_context.whiteboard,
                self._execution_context.whiteboard_api,
                self.whiteboard_id,
                self._channel_manager,
                self._file_serializer,
                UUIDEntryIdGenerator(self.snapshot_id())
            )

    def servant(self) -> Optional[ServantClient]:
        return self._servant_client

    def channel_manager(self) -> ChannelManager:
        return self._channel_manager

    def mem_serializer(self) -> MemBytesSerializer:
        return self._mem_serializer

    def file_serializer(self) -> FileSerializer:
        return self._file_serializer

    def hasher(self) -> Hasher:
        return self._hasher

    def py_env(self, namespace: Optional[Dict[str, Any]] = None) -> PyEnv:
        if self._py_env is not None:
            return self._py_env

        local_module_paths: List[str] = []
        if self._yaml is None:
            if namespace is None:
                name, yaml = create_yaml(installed_packages=all_installed_packages())
            else:
                installed, local_module_paths = select_modules(namespace)
                name, yaml = create_yaml(installed_packages=installed)

            local_modules_uploaded = []
            if self._local_module_paths is None:
                self._local_module_paths = local_module_paths

            for local_module in self._local_module_paths:
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
            self._py_env = PyEnv(name, yaml, local_modules_uploaded)
            return self._py_env

        # TODO: as usually not good idea to read whole file into memory
        # TODO: but right now it's the best option
        with open(self._yaml, "r", encoding=encoding) as file:
            name, yaml_str = "default", "".join(file.readlines())
            data = safe_load(yaml_str)
            self._py_env = PyEnv(data.get('name', 'default'), yaml_str, [])
            return self._py_env


class LzyLocalWorkflow(LzyWorkflowBase):
    def activate(self):
        pass

    def deactivate(self):
        pass

    def __init__(
            self,
            name: str,
            file_serializer: FileSerializer,
            eager: bool = False,
            whiteboard: Any = None,
            buses: Optional[BusList] = None
    ):
        super().__init__(
            name=name,
            whiteboard_api=InMemWhiteboardApi(),
            snapshot_api=InMemSnapshotApi(),
            servant_client=ServantClientMock(),
            eager=eager,
            whiteboard=whiteboard,
            buses=buses
        )
        if self._execution_context.whiteboard is not None:
            check_whiteboard(whiteboard)
            wrap_whiteboard(
                self._execution_context.whiteboard,
                self._execution_context.whiteboard_api,
                self.whiteboard_id,
                LocalChannelManager(""),
                file_serializer,
                UUIDEntryIdGenerator(self.snapshot_id())
            )
