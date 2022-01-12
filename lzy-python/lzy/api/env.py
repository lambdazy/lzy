import dataclasses
import logging
import os
from abc import abstractmethod, ABC
from typing import Dict, List, Tuple, Callable, Type, Any, TypeVar, Iterable, Optional

from lzy.api.buses import Bus
from lzy.api.lazy_op import LzyOp
from lzy.api.pkg_info import all_installed_packages, create_yaml, select_modules
from lzy.api.whiteboard.api import InMemSnapshotApi, InMemWhiteboardApi, \
    SnapshotApi, WhiteboardApi, WhiteboardInfo
from lzy.api.whiteboard.wb import wrap_whiteboard
from lzy.servant.bash_servant_client import BashServantClient
from lzy.servant.servant_client import ServantClient
from lzy.servant.terminal_server import TerminalServer
from lzy.servant.whiteboard_bash_api import SnapshotBashApi, WhiteboardBashApi

T = TypeVar('T')


class LzyEnvBase(ABC):
    @abstractmethod
    def is_active(self) -> bool:
        pass

    @abstractmethod
    def is_local(self) -> bool:
        pass

    @abstractmethod
    def servant(self) -> Optional[ServantClient]:
        pass

    @abstractmethod
    def register_op(self, lzy_op: LzyOp) -> None:
        pass

    @abstractmethod
    def registered_ops(self) -> Iterable[LzyOp]:
        pass

    @abstractmethod
    def get_whiteboard(self, id: str, typ: Type[T]) -> T:
        pass

    @abstractmethod
    def get_all_whiteboards_info(self) -> List[WhiteboardInfo]:
        pass

    @abstractmethod
    def whiteboard_id(self) -> Optional[str]:
        pass

    @abstractmethod
    def snapshot_id(self) -> Optional[str]:
        pass

    @abstractmethod
    def run(self) -> None:
        pass

    @abstractmethod
    def generate_conda_env(self, namespace: Optional[Dict[str, Tuple[str]]]) -> Tuple[str, str]:
        pass


class WhiteboardExecutionContext:
    def __init__(self, whiteboard_api: WhiteboardApi, snapshot_api: SnapshotApi, whiteboard: Any):
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
            self._whiteboard_id = self.whiteboard_api.create([field.name for field in fields], snapshot_id).id
            return self._whiteboard_id
        return None


class LzyEnv(LzyEnvBase):
    instance: Optional['LzyEnv'] = None

    # noinspection PyDefaultArgument
    def __init__(self, eager: bool = False, whiteboard: Any = None,
                 buses: List[Tuple[Callable, Bus]] = [], local: bool = False, user: str = None,
                 yaml_path: str = None, private_key_path: str = '~/.ssh/id_rsa',
                 server_url: str = 'api.lzy.ai:8899',
                 lzy_mount: str = os.getenv("LZY_MOUNT", default="/tmp/lzy")):
        super().__init__()
        if whiteboard is not None and not dataclasses.is_dataclass(whiteboard):
            raise ValueError('Whiteboard should be a dataclass')

        self._local = local
        if not local:
            if not user:
                raise ValueError("Username must be specified")
            self._terminal_server: Optional[TerminalServer] = TerminalServer(private_key_path, lzy_mount, server_url, user)
            self._servant_client: Optional[BashServantClient] = BashServantClient().instance(lzy_mount)
            whiteboard_api: WhiteboardApi = WhiteboardBashApi(lzy_mount, self._servant_client)
            snapshot_api: SnapshotApi = SnapshotBashApi(lzy_mount)
        else:
            self._terminal_server = None
            self._servant_client = None
            whiteboard_api = InMemWhiteboardApi()
            snapshot_api = InMemSnapshotApi()

        self._execution_context = WhiteboardExecutionContext(whiteboard_api, snapshot_api, whiteboard)

        if self._execution_context.whiteboard is not None:
            wrap_whiteboard(self._execution_context.whiteboard, self._execution_context.whiteboard_api, lambda: self.whiteboard_id())

        self._ops: List[LzyOp] = []
        self._eager = eager
        self._buses = list(buses)
        self._yaml = yaml_path
        self._log = logging.getLogger(str(self.__class__))

    def generate_conda_env(self, namespace: Optional[Dict[str, Any]] = None) -> Tuple[str, str]:
        if self._yaml is None:
            if namespace is None:
                return create_yaml(installed_packages=all_installed_packages())

            # are they custom?
            installed, custom_ = select_modules(namespace)
            return create_yaml(installed_packages=installed)


        # TODO: as usually not good idea to read whole file into memory
        # TODO: but right now it's the best option
        # TODO: parse yaml and get name?
        with open(self._yaml, 'r') as file:
            return "default", "".join(file.readlines())

    # TODO: mb better naming
    def already_exists(self):
        cls = type(self)
        return hasattr(cls, 'instance') and cls.instance is not None

    def activate(self):
        # TODO: should it be here or in __exit__?
        if self._terminal_server:
            self._terminal_server.start()
        type(self).instance = self

    def deactivate(self):
        type(self).instance = None
        if self._terminal_server:
            self._terminal_server.stop()

    def whiteboard_id(self) -> Optional[str]:
        return self._execution_context.whiteboard_id

    def snapshot_id(self) -> Optional[str]:
        return self._execution_context.snapshot_id

    def __enter__(self) -> 'LzyEnv':
        if self.already_exists():
            raise ValueError('More than one started lzy environment found')
        self.activate()
        return self

    def __exit__(self, *_) -> None:
        try:
            self.run()
            if self._execution_context._snapshot_id:
                self._execution_context.snapshot_api.finalize(self._execution_context._snapshot_id)
        finally:
            self.deactivate()

    def is_active(self) -> bool:
        return self.already_exists() and type(self).instance is self

    def is_local(self) -> bool:
        return self._local

    def servant(self) -> Optional[ServantClient]:
        return self._servant_client

    def register_op(self, lzy_op: LzyOp) -> None:
        self._ops.append(lzy_op)
        if self._eager:
            lzy_op.materialize()

    def registered_ops(self) -> Iterable[LzyOp]:
        if not self.already_exists():
            raise ValueError('Fetching ops on a non-entered environment')
        return list(self._ops)

    def run(self) -> None:
        if not self.already_exists():
            raise ValueError('Run operation on a non-entered environment')
        for wrapper in self._ops:
            wrapper.materialize()

    @classmethod
    def get_active(cls) -> Optional['LzyEnv']:
        return cls.instance

    def get_whiteboard(self, wid: str, typ: Type[Any]) -> Any:
        if not dataclasses.is_dataclass(typ):
            raise ValueError("Whiteboard must be dataclass")
        # noinspection PyDataclass
        field_types = {field.name: field.type for field in dataclasses.fields(typ)}
        wb = self._execution_context.whiteboard_api.get(wid)
        whiteboard_dict = {
            field.field_name: self._execution_context.whiteboard_api.resolve(
                field.storage_uri, field_types[field.field_name]) for field in  # type: ignore
            wb.fields
        }
        # noinspection PyArgumentList
        return typ(**whiteboard_dict)

    def get_all_whiteboards_info(self) -> List[WhiteboardInfo]:
        return self._execution_context.whiteboard_api.getAll()
