import dataclasses
import inspect
import logging
import os
from abc import abstractmethod, ABC
from pathlib import Path
from typing import List, Tuple, Callable, Type, Any, TypeVar, Iterable, Optional

from lzy.api.pkg_info import get_python_env_as_yaml
from lzy.servant.bash_servant_client import BashServantClient
from lzy.servant.servant_client import ServantClient
from lzy.servant.terminal_server import TerminalServer
from .buses import Bus
from .lazy_op import LzyOp
from .whiteboard import WhiteboardsRepoInMem, WhiteboardControllerImpl

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
    def whiteboards(self, typ: Type[T]) -> Iterable[T]:
        pass

    @abstractmethod
    def projections(self, typ: Type[T]) -> Iterable[T]:
        pass

    @abstractmethod
    def run(self) -> None:
        pass

    @abstractmethod
    def generate_conda_env(self) -> Tuple[str, str]:
        pass


class LzyEnv(LzyEnvBase):
    instance = None

    # noinspection PyDefaultArgument
    def __init__(self, eager: bool = False, whiteboard: Any = None,
                 buses: List[Tuple[Callable, Bus]] = [], local: bool = False, user: str = None,
                 yaml_path: str = None, private_key_path: str = '~/.ssh/id_rsa',
                 server_url: str = 'lzy-kharon.northeurope.cloudapp.azure.com:8899',
                 lzy_mount: str = Path(os.getenv("LZY_MOUNT", default="/tmp/lzy"))):
        super().__init__()
        # if whiteboard is not None and not dataclasses.is_dataclass(whiteboard):
        #     raise ValueError('Whiteboard should be a dataclass')
        if whiteboard is not None:
            self._wb_controller = WhiteboardControllerImpl(whiteboard)
        else:
            self._wb_controller = None

        self._local = local
        if not local:
            if not user:
                raise ValueError("Username must be specified")
            self._terminal_server = TerminalServer(private_key_path, lzy_mount, server_url, user)
            self._servant_client = BashServantClient(lzy_mount)
        else:
            self._terminal_server = None
            self._servant_client = None

        self._wb_repo = WhiteboardsRepoInMem()
        self._ops = []
        self._eager = eager
        self._buses = list(buses)
        self._yaml = yaml_path
        self._log = logging.getLogger(str(self.__class__))

    def generate_conda_env(self) -> Tuple[str, str]:
        if self._yaml is None:
            return get_python_env_as_yaml()

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

    def __enter__(self) -> 'LzyEnv':
        if self.already_exists():
            raise ValueError('More than one started lzy environment found')
        self.activate()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        try:
            self.run()
            if self._wb_controller is not None:
                self._wb_repo.register(self._wb_controller)
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

    def whiteboards(self, typ: Type[T]) -> Iterable[T]:
        return self._wb_repo.whiteboards(typ)

    def projections(self, typ: Type[T]) -> Iterable[T]:
        # TODO: UPDATE with new WB
        wb_arg_name = None
        wb_arg_type = None
        for k, v in inspect.signature(typ).parameters.items():
            if dataclasses.is_dataclass(v.annotation):
                wb_arg_type = v.annotation
                wb_arg_name = k

        if wb_arg_type is None:
            raise ValueError('Projection class should accept whiteboard dataclass as an init argument')

        # noinspection PyArgumentList
        return map(lambda x: typ(**{wb_arg_name: x}), self._wb_repo.whiteboards(wb_arg_type))

    def run(self) -> None:
        if not self.already_exists():
            raise ValueError('Run operation on a non-entered environment')
        if not self._ops:
            raise ValueError('No registered ops')
        for wrapper in self._ops:
            wrapper.materialize()

    @classmethod
    def get_active(cls) -> 'LzyEnv':
        return cls.instance
