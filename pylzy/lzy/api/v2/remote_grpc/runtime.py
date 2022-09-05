import asyncio
import logging
import os
import sys
import time
from asyncio import Task
from collections import defaultdict
from threading import Thread
from typing import Callable, Dict, List, Optional, Tuple, Union

import jwt

from ai.lzy.v1.workflow.workflow_pb2 import Graph, Operation
from lzy.api.v2 import LzyCall, LzyWorkflow
from lzy.api.v2.exceptions import LzyExecutionException
from lzy.api.v2.remote_grpc.workflow_service_client import (
    Completed,
    Executing,
    Failed,
    StdoutMessage,
    WorkflowServiceClient,
)
from lzy.api.v2.runtime import ProgressStep, Runtime

KEY_PATH_ENV = "LZY_KEY_PATH"
LZY_ADDRESS_ENV = "LZY_ADDRESS_ENV"
FETCH_STATUS_PERIOD_SEC = 10


_LOG = logging.getLogger(__name__)


def _build_token(username: str, key_path: Optional[str] = None) -> str:
    if key_path is None:
        key_path = os.getenv(KEY_PATH_ENV)
        if key_path is None:
            raise ValueError(
                f"Key path must be specified by env variable {KEY_PATH_ENV} or in Runtime"
            )

    with open(key_path, "r") as f:
        private_key = f.read()
        return str(
            jwt.encode(
                {  # TODO(artolrod) add renewing of token
                    "iat": time.time(),
                    "nbf": time.time(),
                    "exp": time.time() + 7 * 24 * 60 * 60,  # 7 days
                    "iss": username,
                },
                private_key,
                algorithm="PS256",
            )
        )


class GrpcRuntime(Runtime):
    def __init__(
        self,
        username: str,
        address: Optional[str] = None,
        key_path: Optional[str] = None,
    ):
        self.__username = username
        self.__workflow_address = (
            address
            if address is not None
            else os.getenv(LZY_ADDRESS_ENV, "api.lzy.ai:8899")
        )
        self.__key_path = key_path

        self.__loop = asyncio.get_event_loop()
        self.__loop_thread = Thread(
            name="runtime-thread",
            target=self._run_loop_thread,
            args=(self.__loop,),
            daemon=True,
        )
        self.__loop_thread.start()
        asyncio.set_event_loop(self.__loop)

        self.__workflow_client = WorkflowServiceClient(
            self.__workflow_address, _build_token(username, key_path)
        )
        self.__workflow: Optional[LzyWorkflow] = None
        self.__execution_id: Optional[str] = None

        self.__std_slots_listener: Optional[Task] = None

    def start(self, workflow: LzyWorkflow):
        self.__workflow = workflow
        task = asyncio.run_coroutine_threadsafe(self._start_workflow(), self.__loop)
        try:
            task.result()
        except Exception as e:
            raise RuntimeError(f"Cannot start workflow {workflow.name}") from e

    def exec(
        self,
        graph: List[LzyCall],
        progress: Callable[[ProgressStep], None],
    ) -> None:
        task = asyncio.run_coroutine_threadsafe(self._execute_graph(graph), self.__loop)
        try:
            task.result()
        except LzyExecutionException as e:
            raise e
        except Exception as e:
            raise RuntimeError("Cannot execute graph") from e

    def destroy(self):
        task = asyncio.run_coroutine_threadsafe(self._finish_workflow(), self.__loop)
        try:
            task.result()
        except Exception as e:
            raise RuntimeError("Cannot destroy workflow") from e

    async def _start_workflow(self):
        assert self.__workflow is not None
        _LOG.info(f"Starting workflow {self.__workflow.name}")
        default_creds = self.__workflow.owner.storage_registry.get_default_credentials()

        exec_id, creds = await self.__workflow_client.create_workflow(
            self.__workflow.name, default_creds
        )

        self.__execution_id = exec_id
        if creds is not None:
            self.__workflow.owner.storage_registry.register_credentials(
                exec_id, creds, default=True
            )

        self.__std_slots_listener = asyncio.create_task(
            self._listen_to_std_slots(exec_id)
        )

    async def _listen_to_std_slots(self, execution_id: str):
        async for data in self.__workflow_client.read_std_slots(execution_id):
            if isinstance(data, StdoutMessage):
                print(data.data, file=sys.stdout)
            else:
                print(data.data, file=sys.stderr)

    async def _finish_workflow(self):
        assert self.__execution_id is not None
        assert self.__workflow is not None
        assert self.__std_slots_listener is not None

        _LOG.info(f"Finishing workflow {self.__workflow.name}")

        await self.__workflow_client.finish_workflow(
            self.__workflow.name, self.__execution_id, "Workflow completed"
        )

        self.__workflow.owner.storage_registry.unregister_credentials(
            self.__execution_id
        )

        await self.__std_slots_listener  # read all stdout and stderr

        self.__execution_id = None
        self.__workflow = None
        self.__std_slots_listener = None

    def _build_graph(self, calls: List[LzyCall]) -> Graph:
        assert self.__workflow is not None

        entry_id_to_call: Dict[str, Tuple[LzyCall, int]] = {}
        entry_id_to_output_calls: Dict[
            str, List[Tuple[LzyCall, Union[int, str]]]
        ] = defaultdict(list)
        for call in calls:
            for i, entry_id in enumerate(call.entry_ids):
                entry_id_to_call[entry_id] = (call, i)

            for i, eid in enumerate(call.arg_entry_ids):
                entry_id_to_output_calls[eid].append((call, i))

            for name, eid in call.kwarg_entry_ids.items():
                entry_id_to_output_calls[eid].append((call, name))

        vertices: Dict[str, Graph.VertexDescription] = {}

        for call in calls:
            input_slots: List[str] = []
            output_slots: List[str] = []

            for i, eid in enumerate(call.arg_entry_ids):
                input_slots.append(f"/{call.id}/arg_{i}")

            for name, eid in call.kwarg_entry_ids.items():
                input_slots.append(f"/{call.id}/arg_{name}")

            for i in range(len(call.entry_ids)):
                output_slots.append(f"/{call.id}/ret_{i}")

            docker_image = call.env.base_env.name

            vertices[call.id] = Graph.VertexDescription(
                id=call.id,
                operation=Operation(
                    name=call.signature.func.name,
                    description=call.description,
                    inputSlots=input_slots,
                    outputSlots=output_slots,
                    command="",  # TODO(artolord) add command generation and v2 startup
                    dockerImage=docker_image
                    if docker_image is not None
                    else "",  # TODO(artolord) change env api
                    python=Operation.PythonEnvSpec(  # TODO(artolord) add local modules loading
                        yaml=call.env.aux_env.conda_yaml
                    ),
                    poolSpecName="S",  # TODO(artolord) add label resolving
                ),
            )

        edges: List[Graph.EdgeDescription] = []
        for entry_id in entry_id_to_call.keys():
            entry = self.__workflow.owner.snapshot.get(entry_id)
            edges.append(
                Graph.EdgeDescription(
                    storageUri=entry.storage_url,
                    input=Graph.EdgeDescription.VertexRef(
                        vertexId=entry_id_to_call[entry_id][0].id,
                        slotName=f"/{entry_id_to_call[entry_id][0].id}/ret_{entry_id_to_call[entry_id][1]}",
                    ),
                    outputs=[
                        Graph.EdgeDescription.VertexRef(
                            vertexId=data[0].id, slotName=f"/{data[0].id}/arg_{data[1]}"
                        )
                        for data in entry_id_to_output_calls[entry_id]
                    ],
                    dataScheme=Graph.EdgeDescription.DataScheme(
                        type=entry.data_scheme.type,
                        schemeType=entry.data_scheme.scheme_type,
                    )
                    if entry.data_scheme is not None
                    else None,
                    whiteboardRef=None,  # TODO(artolord) add whiteboards linking
                )
            )

        return Graph(
            name="",
            edges=edges,
            vertices=vertices.values(),
            zone="",  # TODO(artolord) Add zone resolving
        )

    async def _execute_graph(self, calls: List[LzyCall]):
        assert self.__execution_id is not None

        _LOG.info("Building graph")
        graph = self._build_graph(calls)

        # TODO(artolord) add args loading

        graph_id = await self.__workflow_client.execute_graph(
            self.__execution_id, graph
        )
        _LOG.info(f"Send graph to Lzy, graph_id={graph_id}")

        is_executing = False
        while True:
            await asyncio.sleep(FETCH_STATUS_PERIOD_SEC)
            status = await self.__workflow_client.graph_status(
                self.__execution_id, graph_id
            )

            if isinstance(status, Executing) and not is_executing:
                is_executing = True
                continue

            if isinstance(status, Completed):
                _LOG.info(f"Graph {graph_id} execution completed")
                return

            if isinstance(status, Failed):
                _LOG.info(f"Graph {graph_id} execution failed: {status.description}")
                raise LzyExecutionException(
                    f"Failed executing graph {graph_id}: {status.description}"
                )

    @staticmethod
    def _run_loop_thread(loop: asyncio.AbstractEventLoop):
        asyncio.set_event_loop(loop)
        loop.run_forever()
