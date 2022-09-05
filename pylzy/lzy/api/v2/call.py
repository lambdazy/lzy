import typing
import uuid
from typing import Any, Dict, Iterator, Mapping, Sequence, Tuple, TypeVar

from lzy.api.v2.provisioning import Provisioning
from lzy.api.v2.signatures import CallSignature
from lzy.api.v2.utils.proxy_adapter import is_lzy_proxy
from lzy.api.v2.workflow import LzyWorkflow
from lzy.env.env import EnvSpec

T = TypeVar("T")  # pylint: disable=invalid-name


class LzyCall:
    def __init__(
        self,
        parent_wflow: LzyWorkflow,
        sign: CallSignature,
        provisioning: Provisioning,
        env: EnvSpec,
    ):
        self.__id = str(uuid.uuid4())
        self.__wflow = parent_wflow
        self.__sign = sign
        self.__provisioning = provisioning
        self.__env = env
        self.__entry_ids = [
            parent_wflow.snapshot.create_entry(typ).id for typ in sign.func.output_types
        ]

        self.__args_entry_ids: typing.List[str] = []

        for arg in self.__sign.args:
            if is_lzy_proxy(arg):
                self.__args_entry_ids.append(arg.__lzy_entry_id__)
            else:
                self.__args_entry_ids.append(
                    parent_wflow.snapshot.create_entry(type(arg)).id
                )

        self.__kwargs_entry_ids: Dict[str, str] = {}

        for name, kwarg in self.__sign.kwargs.items():
            entry_id: str
            if is_lzy_proxy(kwarg):
                entry_id = kwarg.__lzy_entry_id__
            else:
                entry_id = parent_wflow.snapshot.create_entry(type(kwarg)).id

            self.__kwargs_entry_ids[name] = entry_id

    @property
    def provisioning(self) -> Provisioning:
        return self.__provisioning

    @property
    def env(self) -> EnvSpec:
        return self.__env

    @property
    def parent_wflow(self) -> LzyWorkflow:
        return self.__wflow

    @property
    def signature(self) -> CallSignature:
        return self.__sign

    @property
    def id(self) -> str:
        return self.__id

    @property
    def operation_name(self) -> str:
        return self.__sign.func.name

    @property
    def entry_ids(self) -> Sequence[str]:
        return self.__entry_ids

    @property
    def args(self) -> Tuple[Any, ...]:
        return self.__sign.args

    @property
    def arg_entry_ids(self) -> Sequence[str]:
        return self.__args_entry_ids

    @property
    def kwarg_entry_ids(self) -> Mapping[str, str]:
        return self.kwarg_entry_ids

    @property
    def kwargs(self) -> Dict[str, Any]:
        return self.__sign.kwargs

    def named_arguments(self) -> Iterator[Tuple[str, Any]]:
        return self.__sign.named_arguments()

    @property
    def description(self) -> str:
        return self.__sign.description  # TODO(artolord) Add arguments description here
