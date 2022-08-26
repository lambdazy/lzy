import asyncio
import datetime
import os
import tempfile
import time
import uuid
import jwt
from pathlib import Path
from typing import Any, BinaryIO, Callable, Dict, List, cast, Optional

from lzy._proxy.result import Nothing, Result

from lzy.api.v2 import LzyCall, LzyWorkflow
from lzy.api.v2.remote_grpc.workflow_service_client import WorkflowServiceClient
from lzy.api.v2.runtime import ProgressStep, Runtime
from lzy.serialization.api import SerializersRegistry
from lzy.storage import from_credentials
from lzy.storage.credentials import StorageCredentials


KEY_PATH_ENV = "LZY_KEY_PATH"
LZY_ADDRESS_ENV = "LZY_ADDRESS_ENV"


def _build_token(username: str, key_path: Optional[str] = None) -> str:
    if key_path is None:
        key_path = os.getenv(KEY_PATH_ENV)
        if key_path is None:
            raise ValueError(f"Key path must be specified by env variable {KEY_PATH_ENV} or in Runtime")

    with open(key_path, "r") as f:
        private_key = f.read()
        return str(jwt.encode({  # TODO(artolrod) add renewing of token
            "iat": time.time(),
            "nbf": time.time(),
            "exp": time.time() + 7 * 24 * 60 * 60,  # 7 days
            "iss": username
        }, private_key, algorithm="PS256"))


class GrpcRuntime(Runtime):
    def __init__(
        self,
        username: str,
        address: Optional[str] = None,
        key_path: Optional[str] = None
    ):
        self.__username = username
        self.__workflow_address = address if address is not None else os.getenv(LZY_ADDRESS_ENV, "api.lzy.ai:8899")
        self.__key_path = key_path
        self.__workflow_client = WorkflowServiceClient(self.__workflow_address, _build_token(username, key_path))
        self.__workflow: Optional[LzyWorkflow] = None
        self.__execution_id: Optional[str] = None

    def start(self, workflow: LzyWorkflow):
        self.__workflow = workflow
        asyncio.get_event_loop().run_until_complete(self._start_workflow())

    def exec(
        self,
        graph: List[LzyCall],
        progress: Callable[[ProgressStep], None],
    ) -> None:
        pass

    def resolve_data(self, entry_id: str) -> Result[Any]:
        return Nothing()

    def destroy(self):
        asyncio.get_event_loop().run_until_complete(self._finish_workflow())

    async def _start_workflow(self):
        assert self.__workflow is not None
        default_creds = self.__workflow.owner.storage_registry.get_default_credentials()

        exec_id, creds = await self.__workflow_client.create_workflow(self.__workflow.name, default_creds)

        self.__execution_id = exec_id
        if creds is not None:
            self.__workflow.owner.storage_registry.register_credentials(exec_id, creds, default=True)

    async def _finish_workflow(self):
        assert self.__execution_id is not None
        assert self.__workflow is not None

        await self.__workflow_client.finish_workflow(self.__workflow.name, self.__execution_id, "Workflow completed")

        self.__workflow.owner.storage_registry.unregister_credentials(self.__execution_id)

        self.__execution_id = None
        self.__workflow = None
