import dataclasses
import logging
import os
import tempfile
from abc import abstractmethod, ABC
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Tuple, Callable, Type, Any, TypeVar, Iterable, Optional

from lzy.api.buses import Bus
from lzy.api.lazy_op import LzyOp
from lzy.api.pkg_info import all_installed_packages, create_yaml, select_modules
import zipfile
from lzy.api.utils import zipdir, fileobj_hash
from lzy.api.storage.storage_client import StorageClient, from_credentials
from lzy.api.whiteboard import check_whiteboard, wrap_whiteboard, wrap_whiteboard_for_read
from lzy.api.whiteboard.model import (
    InMemSnapshotApi,
    InMemWhiteboardApi,
    SnapshotApi,
    WhiteboardApi,
    WhiteboardList,
    WhiteboardDescription,
    WhiteboardFieldStatus
)
from lzy.model.encoding import ENCODING as encoding
from lzy.model.env import PyEnv
from lzy.servant.bash_servant_client import BashServantClient
from lzy.servant.servant_client import ServantClient, CredentialsTypes
from lzy.servant.whiteboard_bash_api import SnapshotBashApi, WhiteboardBashApi

T = TypeVar("T")  # pylint: disable=invalid-name
BusList = List[Tuple[Callable, Bus]]


class LzyEnvBase(ABC):
    instances: List["LzyEnvBase"] = []

    # pylint: disable=too-many-arguments
    def __init__(self,
                 buses: Optional[BusList],
                 whiteboard: Any,
                 whiteboard_api: WhiteboardApi,
                 snapshot_api: SnapshotApi,
                 eager: bool):
        self._execution_context = WhiteboardExecutionContext(
            whiteboard_api, snapshot_api, whiteboard
        )

        if self._execution_context.whiteboard is not None:
            check_whiteboard(whiteboard)
            wrap_whiteboard(
                self._execution_context.whiteboard,
                self._execution_context.whiteboard_api,
                self.whiteboard_id,
            )

        self._ops: List[LzyOp] = []
        self._buses = list(buses or [])
        self._eager = eager
        self._log = logging.getLogger(str(self.__class__))

    def whiteboard(self, wid: str, typ: Type[Any]) -> Any:
        check_whiteboard(typ)
        wb_ = self._execution_context.whiteboard_api.get(wid)
        return self._build_whiteboard(wb_, typ)

    def _build_whiteboard(self, wb_: WhiteboardDescription, typ: Type[Any]) -> Any:
        check_whiteboard(typ)
        # noinspection PyDataclass
        field_types = {field.name: field.type for field in dataclasses.fields(typ)}
        whiteboard_dict: Dict[str, Any] = {}
        for field in wb_.fields:
            if field.field_name in field_types:
                if field.status is WhiteboardFieldStatus.FINISHED:
                    whiteboard_dict[field.field_name] = self._execution_context \
                        .whiteboard_api \
                        .resolve(field.storage_uri, field_types[field.field_name])
        # noinspection PyArgumentList
        instance = typ(**whiteboard_dict)
        wrap_whiteboard_for_read(instance, wb_)
        return instance

    def _whiteboards(self, namespace: str, tags: List[str], typ: Type[T], from_date: datetime = None,
                     to_date: datetime = None) -> List[T]:
        check_whiteboard(typ)
        wb_list = self._execution_context.whiteboard_api.list(namespace, tags, from_date, to_date)
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

    def register_op(self, lzy_op: LzyOp) -> None:
        self._ops.append(lzy_op)
        if self._eager:
            lzy_op.materialize()

    def whiteboard_id(self) -> Optional[str]:
        return self._execution_context.whiteboard_id

    def snapshot_id(self) -> Optional[str]:
        return self._execution_context.snapshot_id

    def registered_ops(self) -> Iterable[LzyOp]:
        # if self.get_active() is None:
        #    raise ValueError("Fetching ops on a non-entered environment")
        return list(self._ops)

    def run(self) -> None:
        assert self.get_active() is not None
        for wrapper in self._ops:
            wrapper.materialize()

    @abstractmethod
    def activate(self):
        pass

    @abstractmethod
    def deactivate(self):
        pass

    @classmethod
    def get_active(cls) -> "LzyEnvBase":
        assert len(cls.instances) > 0, "There is not active LzyEnv"
        return cls.instances[-1]

    def __enter__(self) -> "LzyEnvBase":
        self.activate()
        type(self).instances.append(self)
        return self

    def __exit__(self, *_) -> None:
        try:
            self.run()
            context = self._execution_context
            # pylint: disable=protected-access
            # noinspection PyProtectedMember
            if context._snapshot_id is not None:
                # noinspection PyProtectedMember
                context.snapshot_api.finalize(context._snapshot_id)
        finally:
            self.deactivate()
            type(self).instances.pop()


class WhiteboardExecutionContext:
    def __init__(
            self, whiteboard_api: WhiteboardApi, snapshot_api: SnapshotApi, whiteboard: Any
    ):
        self._snapshot_id: Optional[str] = None
        self._whiteboard_id: Optional[str] = None
        self.whiteboard_api = whiteboard_api
        self.snapshot_api = snapshot_api
        self.whiteboard = whiteboard

    @property
    def snapshot_id(self) -> Optional[str]:
        if self._snapshot_id is not None:
            return self._snapshot_id
        self._snapshot_id = self.snapshot_api.create().snapshot_id
        return self._snapshot_id

    @property
    def whiteboard_id(self) -> Optional[str]:
        if self._whiteboard_id is not None:
            return self._whiteboard_id
        if self.whiteboard_api is not None and self.whiteboard is not None:
            fields = dataclasses.fields(self.whiteboard)
            snapshot_id = self.snapshot_id
            if snapshot_id is None:
                raise RuntimeError("Cannot create snapshot")
            self._whiteboard_id = self.whiteboard_api.create(
                [field.name for field in fields], snapshot_id,
                self.whiteboard.LZY_WB_NAMESPACE, self.whiteboard.LZY_WB_TAGS
            ).id
            return self._whiteboard_id
        return None


class LzyLocalEnv(LzyEnvBase):
    def __init__(
            self,
            eager: bool = False,
            whiteboard: Any = None,
            buses: Optional[BusList] = None,
    ):
        super().__init__(
            buses=buses,
            whiteboard=whiteboard,
            whiteboard_api=InMemWhiteboardApi(),
            snapshot_api=InMemSnapshotApi(),
            eager=eager
        )

    def activate(self):
        pass

    def deactivate(self):
        pass


class LzyRemoteEnv(LzyEnvBase):
    def __init__(
            self,
            lzy_mount: str = os.getenv("LZY_MOUNT", default="/tmp/lzy"),
            eager: bool = False,
            conda_yaml_path: Optional[Path] = None,
            whiteboard: Any = None,
            buses: Optional[BusList] = None,
            local_module_paths: Optional[List[str]] = None
    ):
        self._yaml = conda_yaml_path
        self._servant_client: BashServantClient = BashServantClient() \
            .instance(lzy_mount)
        self._py_env: Optional[PyEnv] = None

        bucket = self._servant_client.get_bucket()
        self._bucket = bucket

        credentials = self._servant_client.get_credentials(CredentialsTypes.S3, bucket)
        self._storage_client: StorageClient = from_credentials(credentials)
        if local_module_paths is None:
            self._local_module_paths: List[str] = []
        else:
            self._local_module_paths = local_module_paths

        super().__init__(
            buses=buses,
            whiteboard=whiteboard,
            whiteboard_api=WhiteboardBashApi(lzy_mount, self._servant_client),
            snapshot_api=SnapshotBashApi(lzy_mount),
            eager=eager
        )

    def activate(self):
        pass

    def deactivate(self):
        pass

    def servant(self) -> Optional[ServantClient]:
        return self._servant_client

    def py_env(self, namespace: Optional[Dict[str, Any]] = None) -> PyEnv:
        if self._py_env is not None:
            return self._py_env
        if self._yaml is None:
            if namespace is None:
                name, yaml = create_yaml(installed_packages=all_installed_packages())
            else:
                installed, _ = select_modules(namespace)
                name, yaml = create_yaml(installed_packages=installed)

            local_modules_uploaded = []

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
                    uri = self._storage_client.write(self._bucket, key, archive)  # type: ignore
                local_modules_uploaded.append((os.path.basename(local_module), uri))
            self._py_env = PyEnv(name, yaml, local_modules_uploaded)
            return self._py_env

        # TODO: as usually not good idea to read whole file into memory
        # TODO: but right now it's the best option
        # TODO: parse yaml and get name?
        with open(self._yaml, "r", encoding=encoding) as file:
            name, yaml = "default", "".join(file.readlines())
            self._py_env = PyEnv(name, yaml, [])
            return self._py_env
