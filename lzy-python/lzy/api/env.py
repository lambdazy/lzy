import dataclasses
import logging
from abc import abstractmethod, ABC
from types import ModuleType
from typing import Dict, List, Tuple, Callable, Type, Any, TypeVar, Iterable, Optional, Set

from lzy.api.buses import Bus
from lzy.api.lazy_op import LzyOp
from lzy.api.pkg_info import all_installed_packages, create_yaml, select_modules
from lzy.api.whiteboard.api import (
    InMemSnapshotApi,
    InMemWhiteboardApi,
    SnapshotApi,
    WhiteboardApi,
    WhiteboardInfo,
)
from lzy.api.whiteboard.wb import wrap_whiteboard
from lzy.model.encoding import ENCODING as encoding
from lzy.model.env import PyEnv
from lzy.servant.bash_servant_client import BashServantClient
from lzy.servant.servant_client import ServantClient
from lzy.servant.terminal_server import TerminalServer, TerminalConfig
from lzy.servant.whiteboard_bash_api import SnapshotBashApi, WhiteboardBashApi

T = TypeVar("T")  # pylint: disable=invalid-name
BusList = List[Tuple[Callable, Bus]]


class LzyEnvBase(ABC):
    instances: List["LzyEnvBase"] = []

    # pylint: disable=too-many-arguments
    def __init__(self,
                 buses: BusList,
                 whiteboard: Any,
                 whiteboard_api: WhiteboardApi,
                 snapshot_api: SnapshotApi,
                 eager: bool):
        self._execution_context = WhiteboardExecutionContext(
            whiteboard_api, snapshot_api, whiteboard
        )

        if self._execution_context.whiteboard is not None:
            wrap_whiteboard(
                self._execution_context.whiteboard,
                self._execution_context.whiteboard_api,
                self.whiteboard_id,
            )

        self._ops: List[LzyOp] = []
        self._buses = list(buses)
        self._eager = eager
        self._log = logging.getLogger(str(self.__class__))

    def get_whiteboard(self, wid: str, typ: Type[Any]) -> Any:
        if not dataclasses.is_dataclass(typ):
            raise ValueError("Whiteboard must be dataclass")

        # noinspection PyDataclass
        field_types = {field.name: field.type for field in dataclasses.fields(typ)}
        wb_ = self._execution_context.whiteboard_api.get(wid)
        whiteboard_dict = {}
        for field in wb_.fields:
            assert field.storage_uri is not None
            whiteboard_dict[field.field_name] = self._execution_context \
                .whiteboard_api \
                .resolve(field.storage_uri, field_types[field.field_name])

        # noinspection PyArgumentList
        return typ(**whiteboard_dict)

    def get_all_whiteboards_info(self) -> List[WhiteboardInfo]:
        return self._execution_context.whiteboard_api.get_all()

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
                [field.name for field in fields], snapshot_id
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
        if whiteboard is not None and not dataclasses.is_dataclass(whiteboard):
            raise ValueError("Whiteboard should be a dataclass")
        buses = buses or []

        whiteboard_api = InMemWhiteboardApi()
        snapshot_api = InMemSnapshotApi()
        super().__init__(buses, whiteboard, whiteboard_api, snapshot_api, eager)

    def activate(self):
        pass

    def deactivate(self):
        pass


class LzyRemoteEnv(LzyEnvBase):
    def __init__(
            self,
            eager: bool = False,
            whiteboard: Any = None,
            buses: Optional[BusList] = None,
            config: Optional[TerminalConfig] = None,
    ):
        config_: TerminalConfig = config or TerminalConfig()  # type: ignore
        buses = buses or []
        if whiteboard is not None and not dataclasses.is_dataclass(whiteboard):
            raise ValueError("Whiteboard should be a dataclass")

        if config_.user is None:
            raise ValueError("Username must be specified")
        self._terminal_server: TerminalServer = TerminalServer(config_)
        self._servant_client: BashServantClient = BashServantClient().instance(config_.lzy_mount)
        whiteboard_api: WhiteboardApi = WhiteboardBashApi(
            config_.lzy_mount, self._servant_client
        )
        snapshot_api: SnapshotApi = SnapshotBashApi(config_.lzy_mount)
        super().__init__(buses, whiteboard, whiteboard_api, snapshot_api, eager)
        self._yaml = config_.yaml_path

    def py_env(self, namespace: Optional[Dict[str, Any]] = None) -> PyEnv:
        if self._yaml is None:
            if namespace is None:
                name, yaml = create_yaml(installed_packages=all_installed_packages())
                local_modules: Set[ModuleType] = set()
            else:
                installed, local_modules = select_modules(namespace)
                name, yaml = create_yaml(installed_packages=installed)
            return PyEnv(name, yaml, local_modules)

        # TODO: as usually not good idea to read whole file into memory
        # TODO: but right now it's the best option
        # TODO: parse yaml and get name?
        with open(self._yaml, "r", encoding=encoding) as file:
            name, yaml = "default", "".join(file.readlines())
            return PyEnv(name, yaml, [])

    def activate(self):
        self._terminal_server.start()

    def deactivate(self):
        self._terminal_server.stop()

    def servant(self) -> Optional[ServantClient]:
        return self._servant_client
