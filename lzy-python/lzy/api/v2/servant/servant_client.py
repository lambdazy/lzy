import logging
from abc import ABC, abstractmethod
from enum import Enum
from pathlib import Path
from typing import Iterable, List

from lzy.storage.credentials import StorageCredentials, AmazonCredentials
from lzy.api.v2.servant.model.channel import Channel, Bindings
from lzy.api.v2.servant.model.execution import Execution, ExecutionDescription, InputExecutionValue
from lzy.api.v2.servant.model.slot import Slot
from lzy.api.v2.servant.model.zygote import Zygote


class CredentialsTypes(Enum):
    S3 = "s3"


class ServantClient(ABC):
    def __init__(self):
        super().__init__()
        self._log = logging.getLogger(str(self.__class__))

    @abstractmethod
    def mount(self) -> Path:
        pass

    @abstractmethod
    def get_slot_path(self, slot: Slot) -> Path:
        pass

    @abstractmethod
    def create_channel(self, channel: Channel):
        pass

    @abstractmethod
    def destroy_channel(self, channel: Channel):
        pass

    @abstractmethod
    def touch(self, slot: Slot, channel: Channel):
        pass

    @abstractmethod
    def publish(self, zygote: Zygote):
        pass

    @abstractmethod
    def run(
            self,
            execution_id: str,
            zygote: Zygote,
            bindings: Bindings) -> Execution:
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
            self,
            name: str,
            snapshot_id: str,
            inputs: Iterable[InputExecutionValue]) -> List[ExecutionDescription]:
        pass

    def _zygote_path(self, zygote: Zygote) -> str:
        return f"{self.mount()}/bin/{zygote.name}"


class ServantClientMock(ServantClient):
    def save_execution(self, execution: ExecutionDescription):
        pass

    def resolve_executions(self, name: str, snapshot_id: str, inputs: Iterable[InputExecutionValue]) \
            -> List[ExecutionDescription]:
        return []

    def mount(self) -> Path:
        pass

    def get_slot_path(self, slot: Slot) -> Path:
        pass

    def create_channel(self, channel: Channel):
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
        return AmazonCredentials("https://service.us-west-2.amazonaws.com", "access_token", "secret_token")

    def get_bucket(self) -> str:
        return "bucket"
