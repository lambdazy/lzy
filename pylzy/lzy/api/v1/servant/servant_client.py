import logging
from abc import ABC, abstractmethod
from enum import Enum
from pathlib import Path
from typing import Iterable, List, Union

from lzy.api.v1.servant.model.channel import (
    Bindings,
    Channel,
    DataSchema,
    DirectChannelSpec,
    SnapshotChannelSpec,
)
from lzy.api.v1.servant.model.execution import (
    Execution,
    ExecutionDescription,
    InputExecutionValue,
)
from lzy.api.v1.servant.model.slot import Slot
from lzy.api.v1.servant.model.zygote import Zygote
from lzy.storage.api import AmazonCredentials, StorageCredentials


class CredentialsTypes(Enum):
    S3 = "s3"


class ServantClient(ABC):
    def __init__(self) -> None:
        super().__init__()
        self._log = logging.getLogger(str(self.__class__))

    @abstractmethod
    def mount(self) -> Path:
        pass

    @abstractmethod
    def get_slot_path(self, slot: Slot) -> Path:
        pass

    @abstractmethod
    def create_channel(
        self,
        name: str,
        data_schema: DataSchema,
        spec: Union[SnapshotChannelSpec, DirectChannelSpec],
    ) -> Channel:
        pass

    @abstractmethod
    def destroy_channel(self, channel: Channel) -> None:
        pass

    @abstractmethod
    def touch(self, slot: Slot, channel: Channel) -> None:
        pass

    @abstractmethod
    def publish(self, zygote: Zygote) -> None:
        pass

    @abstractmethod
    def run(self, execution_id: str, zygote: Zygote, bindings: Bindings) -> Execution:
        pass

    @abstractmethod
    def get_credentials(self, typ: CredentialsTypes, bucket: str) -> StorageCredentials:
        pass

    @abstractmethod
    def get_bucket(self) -> str:
        pass

    @abstractmethod
    def save_execution(self, execution: ExecutionDescription):
        pass

    @abstractmethod
    def resolve_executions(
        self, name: str, snapshot_id: str, inputs: Iterable[InputExecutionValue]
    ) -> List[ExecutionDescription]:
        pass

    def _zygote_path(self, zygote: Zygote) -> str:
        return f"{self.mount()}/bin/{zygote.name}"


class ServantClientMock(ServantClient):
    def save_execution(self, execution: ExecutionDescription):
        pass

    def resolve_executions(
        self, name: str, snapshot_id: str, inputs: Iterable[InputExecutionValue]
    ) -> List[ExecutionDescription]:
        return []

    def mount(self) -> Path:
        pass

    def get_slot_path(self, slot: Slot) -> Path:
        pass

    def create_channel(
        self,
        name: str,
        data_schema: DataSchema,
        spec: Union[SnapshotChannelSpec, DirectChannelSpec],
    ) -> Channel:
        pass

    def destroy_channel(self, channel: Channel):
        pass

    def touch(self, slot: Slot, channel: Channel):
        pass

    def publish(self, zygote: Zygote):
        pass

    def run(self, execution_id: str, zygote: Zygote, bindings: Bindings) -> Execution:
        pass

    def get_credentials(self, typ: CredentialsTypes, bucket: str) -> StorageCredentials:
        return AmazonCredentials(
            "https://service.us-west-2.amazonaws.com", "access_token", "secret_token"
        )

    def get_bucket(self) -> str:
        return "bucket"
