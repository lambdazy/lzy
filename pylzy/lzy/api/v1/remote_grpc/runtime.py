import asyncio
import functools
import logging
import os
import sys
import tempfile
import zipfile
from asyncio import Task
from io import BytesIO
from typing import (
    Callable,
    Dict,
    Iterable,
    List,
    Optional,
    Sequence,
    Set,
    Tuple,
    Type,
    cast,
)

from ai.lzy.v1.common.data_scheme_pb2 import DataScheme
from ai.lzy.v1.workflow.workflow_pb2 import (
    DataDescription,
    Graph,
    Operation,
    VmPoolSpec,
)
from lzy.api.v1.call import LzyCall
from lzy.api.v1.workflow import LzyWorkflow
from lzy.api.v1.provisioning import Provisioning
from lzy.api.v1.exceptions import LzyExecutionException
from lzy.serialization.api import Schema
from lzy.utils.grpc import build_token
from lzy.api.v1.remote_grpc.workflow_service_client import (
    Completed,
    Executing,
    Failed,
    StdoutMessage,
    WorkflowServiceClient,
)
from lzy.api.v1.runtime import (
    ProgressStep,
    Runtime,
)
from lzy.api.v1.whiteboard_declaration import WhiteboardField, WhiteboardInstanceMeta
from lzy.api.v1.startup import ProcessingRequest
from lzy.api.v1.utils._pickle import pickle
from lzy.api.v1.utils.files import fileobj_hash, zipdir
from lzy.api.v1.workflow import WbRef

from ai.lzy.v1.workflow.workflow_pb2 import WhiteboardField as Wb


LZY_ADDRESS_ENV = "LZY_ADDRESS_ENV"
FETCH_STATUS_PERIOD_SEC = float(os.getenv("FETCH_STATUS_PERIOD_SEC", "10"))

_LOG = logging.getLogger(__name__)


def wrap_error(message: str = "Something went wrong"):
    def decorator(f):
        @functools.wraps(f)
        async def wrap(*args, **kwargs):
            try:
                return await f(*args, **kwargs)
            except LzyExecutionException as e:
                raise e
            except Exception as e:
                raise RuntimeError(message) from e

        return wrap

    return decorator


class GrpcRuntime(Runtime):
    def __init__(
        self,
        username: Optional[str] = None,
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

        self.__workflow_client: Optional[WorkflowServiceClient] = None
        self.__workflow: Optional[LzyWorkflow] = None
        self.__execution_id: Optional[str] = None

        self.__std_slots_listener: Optional[Task] = None
        self.__running = False
        self.__loaded_modules: Set[str] = set()

    async def start(self, workflow: LzyWorkflow):
        self.__running = True
        self.__workflow = workflow
        client = await self.__get_client()

        _LOG.info(f"Starting workflow {self.__workflow.name}")
        default_creds = self.__workflow.owner.storage_registry.default_config()

        exec_id, creds = await client.create_workflow(
            self.__workflow.name, default_creds
        )

        self.__execution_id = exec_id
        if creds is not None:
            self.__workflow.owner.storage_registry.register_storage(
                exec_id, creds, default=True
            )

        self.__std_slots_listener = asyncio.create_task(
            self.__listen_to_std_slots(exec_id)
        )

    async def exec(
        self,
        calls: List[LzyCall],
        links: Dict[str, WbRef],
        progress: Callable[[ProgressStep], None],
    ) -> None:
        assert self.__execution_id is not None

        client = await self.__get_client()
        pools = await client.get_pool_specs(self.__execution_id)

        modules: Set[str] = set()
        for call in calls:
            modules.update(call.env.local_modules)

        urls = await self.__load_local_modules(modules)

        _LOG.info("Building graph")
        print(links)
        graph = await asyncio.get_event_loop().run_in_executor(
            None, self.__build_graph, calls, pools, zip(modules, urls), links
        )  # Running long op in threadpool
        _LOG.debug(f"Starting executing graph {graph}")

        graph_id = await client.execute_graph(self.__execution_id, graph)
        _LOG.info(f"Send graph to Lzy, graph_id={graph_id}")

        is_executing = False
        while True:
            await asyncio.sleep(FETCH_STATUS_PERIOD_SEC)
            status = await client.graph_status(self.__execution_id, graph_id)

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

    async def destroy(self):
        client = await self.__get_client()
        _LOG.info(f"Finishing workflow {self.__workflow.name}")

        try:
            if not self.__running:
                return

            assert self.__execution_id is not None
            assert self.__workflow is not None
            assert self.__std_slots_listener is not None

            await client.finish_workflow(
                self.__workflow.name, self.__execution_id, "Workflow completed"
            )

            await self.__std_slots_listener  # read all stdout and stderr

            self.__execution_id = None
            self.__workflow = None
            self.__std_slots_listener = None

        finally:
            await client.stop()
            self.__workflow_client = None
            self.__running = False

    async def create_whiteboard(
        self,
        namespace: str,
        name: str,
        fields: Sequence[WhiteboardField],
        storage_name: str,
        tags: Sequence[str],
    ) -> WhiteboardInstanceMeta:

        f = [
            Wb(
               name=field.name,
               default=None if field.default is None else Wb.DefaultFieldDesc(
                   uri=field.default.url,
                   dataScheme=DataScheme(
                       dataFormat=field.default.data_scheme.data_format,
                       schemeFormat=field.default.data_scheme.schema_format,
                       schemeContent=field.default.data_scheme.schema_content
                       if field.default.data_scheme.schema_content else "",
                       metadata=field.default.data_scheme.meta
                   )
               )
            ) for field in fields
        ]

        client = await self.__get_client()
        _LOG.info(f"Creating whiteboard {namespace}:{name}")
        whiteboard_id = await client.create_whiteboard(namespace, name, f, storage_name, tags)
        _LOG.info(f"Whiteboard created {namespace}:{name}")
        return WhiteboardInstanceMeta(id=whiteboard_id)

    async def link(self, wb_id: str, field_name: str, url: str, data_scheme: Schema) -> None:
        client = await self.__get_client()
        _LOG.info(f"Linking whiteboard field {wb_id}:{field_name} to {url}")
        await client.link_whiteboard(whiteboard_id=wb_id, field_name=field_name, storage_uri=url, data_scheme=data_scheme)

    async def __load_local_modules(self, module_paths: Iterable[str]) -> Sequence[str]:
        """Returns sequence of urls"""
        assert self.__workflow is not None

        modules_uploaded = []
        for local_module in module_paths:

            if os.path.basename(local_module) in self.__loaded_modules:
                continue

            with tempfile.NamedTemporaryFile("rb") as archive:
                if not os.path.isdir(local_module):
                    with zipfile.ZipFile(archive.name, "w") as z:
                        z.write(local_module, os.path.relpath(local_module))
                else:
                    with zipfile.ZipFile(archive.name, "w") as z:
                        zipdir(local_module, z)
                archive.seek(0)
                file = cast(BytesIO, archive.file)
                key = os.path.join(
                    "local_modules",
                    os.path.basename(local_module),
                    fileobj_hash(file),
                )
                file.seek(0)
                client = self.__workflow.owner.storage_registry.default_client()
                if client is None:
                    raise RuntimeError("No default storage config")

                conf = self.__workflow.owner.storage_registry.default_config()
                if conf is None:
                    raise RuntimeError("No default storage config")

                url = client.generate_uri(conf.bucket, key)

                if not await client.blob_exists(url):
                    await client.write(url, file)

                presigned_uri = await client.sign_storage_uri(url)
                modules_uploaded.append(presigned_uri)

            self.__loaded_modules.add(os.path.basename(local_module))

        return modules_uploaded

    @staticmethod
    def __resolve_pool(
        provisioning: Provisioning, pool_specs: Sequence[VmPoolSpec]
    ) -> Optional[VmPoolSpec]:
        assert (
            provisioning.cpu_type is not None
            and provisioning.cpu_count is not None
            and provisioning.gpu_type is not None
            and provisioning.gpu_count is not None
            and provisioning.ram_size_gb is not None
        )
        for spec in pool_specs:
            if (
                provisioning.cpu_type == spec.cpuType
                and provisioning.cpu_count <= spec.cpuCount
                and provisioning.gpu_type == spec.gpuType
                and provisioning.gpu_count <= spec.gpuCount
                and provisioning.ram_size_gb <= spec.ramGb
            ):
                return spec
        return None

    async def __get_client(self):
        if self.__workflow_client is None:
            self.__workflow_client = await WorkflowServiceClient.create(
                self.__workflow_address, build_token(self.__username, self.__key_path)
            )
        return self.__workflow_client

    async def __listen_to_std_slots(self, execution_id: str):
        client = await self.__get_client()
        async for data in client.read_std_slots(execution_id):
            if isinstance(data, StdoutMessage):
                print(data.data, file=sys.stdout)
            else:
                print(data.data, file=sys.stderr)

    def __build_graph(
        self,
        calls: List[LzyCall],
        pools: Sequence[VmPoolSpec],
        modules: Iterable[Tuple[str, str]],
        links: Dict[str, WbRef]
    ) -> Graph:
        assert self.__workflow is not None

        operations: List[Operation] = []
        data_descriptions: Dict[str, DataDescription] = {}
        pool_to_call: List[Tuple[VmPoolSpec, LzyCall]] = []

        for call in calls:
            input_slots: List[Operation.SlotDescription] = []
            output_slots: List[Operation.SlotDescription] = []
            arg_descriptions: List[Tuple[Type, str]] = []
            kwarg_descriptions: Dict[str, Tuple[Type, str]] = {}
            ret_descriptions: List[str] = []

            for i, eid in enumerate(call.arg_entry_ids):
                entry = self.__workflow.snapshot.get(eid)
                slot_path = f"/{call.id}/arg_{i}"
                input_slots.append(
                    Operation.SlotDescription(
                        path=slot_path, storageUri=entry.storage_url
                    )
                )
                arg_descriptions.append((entry.typ, slot_path))

                scheme = entry.data_scheme

                data_descriptions[entry.storage_url] = DataDescription(
                    storageUri=entry.storage_url,
                    dataScheme=DataScheme(
                        dataFormat=scheme.data_format,
                        schemeFormat=scheme.schema_format,
                        schemeContent=scheme.schema_content if scheme.schema_content else "",
                        metadata=entry.data_scheme.meta
                    )
                    if entry.data_scheme is not None
                    else None,
                )

            for name, eid in call.kwarg_entry_ids.items():
                entry = self.__workflow.snapshot.get(eid)
                slot_path = f"/{call.id}/arg_{name}"
                input_slots.append(
                    Operation.SlotDescription(
                        path=slot_path, storageUri=entry.storage_url
                    )
                )
                kwarg_descriptions[name] = (entry.typ, slot_path)

                data_descriptions[entry.storage_url] = DataDescription(
                    storageUri=entry.storage_url,
                    dataScheme=DataScheme(
                        dataFormat=entry.data_scheme.data_format,
                        schemeFormat=entry.data_scheme.schema_format,
                        schemeContent=entry.data_scheme.schema_content if entry.data_scheme.schema_content else "",
                        metadata=entry.data_scheme.meta
                    )
                    if entry.data_scheme is not None
                    else None,
                )

            for i, eid in enumerate(call.entry_ids):
                slot_path = f"/{call.id}/ret_{i}"
                entry = self.__workflow.snapshot.get(eid)
                output_slots.append(
                    Operation.SlotDescription(
                        path=slot_path, storageUri=entry.storage_url
                    )
                )

                res = links.pop(entry.storage_url, None)

                wb_ref: Optional[DataDescription.WhiteboardRef] = None

                if res is not None:
                    wb_ref = DataDescription.WhiteboardRef(
                        whiteboardId=res.whiteboard_id,
                        fieldName=res.field_name
                    )

                data_descriptions[entry.storage_url] = DataDescription(
                    storageUri=entry.storage_url,
                    dataScheme=DataScheme(
                        dataFormat=entry.data_scheme.data_format,
                        schemeFormat=entry.data_scheme.schema_format,
                        schemeContent=entry.data_scheme.schema_content if entry.data_scheme.schema_content else "",
                        metadata=entry.data_scheme.meta
                    )
                    if entry.data_scheme is not None
                    else None,
                    whiteboardRef=wb_ref
                )
                ret_descriptions.append(slot_path)

            pool = self.__resolve_pool(call.provisioning, pools)

            if pool is None:
                raise RuntimeError(
                    f"Cannot resolve pool for operation {call.signature.func.name}"
                )

            pool_to_call.append((pool, call))

            docker_image: Optional[str]

            if call.env.docker:
                docker_image = call.env.docker.image
            else:
                docker_image = None

            request = ProcessingRequest(
                serializers=self.__workflow.owner.serializer,
                op=call.signature.func.callable,
                args_paths=arg_descriptions,
                kwargs_paths=kwarg_descriptions,
                output_paths=ret_descriptions,
            )

            _com = "".join(
                [
                    "python ",
                    "$(python -c 'import site; print(site.getsitepackages()[0])')",
                    "/lzy/api/v1/startup.py ",
                ]
            )

            command = _com + " " + pickle(request)

            operations.append(
                Operation(
                    name=call.signature.func.name,
                    description=call.description,
                    inputSlots=input_slots,
                    outputSlots=output_slots,
                    command=command,
                    dockerImage=docker_image if docker_image is not None else "",
                    python=Operation.PythonEnvSpec(
                        yaml=call.env.conda.yaml,
                        localModules=[
                            Operation.PythonEnvSpec.LocalModule(name=name, url=url)
                            for (name, url) in modules
                        ],
                    ),
                    poolSpecName=pool.poolSpecName,
                )
            )

        if self.__workflow.is_interactive:  # TODO(artolord) add costs
            s = ""
            for pool, call in pool_to_call:
                s += f"Call to op {call.signature.func.name} mapped to pool {pool.poolSpecName}\n"

            s += "Are you sure you want to run the graph with this configuration?"

            print(s)
            s = input("(Yes/[No]): ")
            if s != "Yes":
                raise RuntimeError("Graph execution cancelled")

        return Graph(
            name="",
            dataDescriptions=list(data_descriptions.values()),
            operations=operations,
            zone="",
        )
