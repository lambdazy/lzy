import asyncio
import functools
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
from lzy.api.v1.exceptions import LzyExecutionException
from lzy.api.v1.provisioning import Provisioning
from lzy.api.v1.remote.workflow_service_client import (
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
from lzy.api.v1.startup import ProcessingRequest
from lzy.api.v1.utils.conda import generate_conda_yaml
from lzy.api.v1.utils.files import fileobj_hash, zipdir
from lzy.api.v1.utils.pickle import pickle
from lzy.api.v1.workflow import LzyWorkflow
from lzy.logs.config import get_logger, get_logging_config, RESET_COLOR, COLOURS, get_color
from lzy.utils.grpc import build_token

FETCH_STATUS_PERIOD_SEC = float(os.getenv("FETCH_STATUS_PERIOD_SEC", "10"))
KEY_PATH_ENV = "LZY_KEY_PATH"
USER_ENV = "LZY_USER"
ENDPOINT_ENV = "LZY_ENDPOINT"

_LOG = get_logger(__name__)


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


class RemoteRuntime(Runtime):
    def __init__(self):
        self.__workflow_client: Optional[WorkflowServiceClient] = None
        self.__workflow: Optional[LzyWorkflow] = None
        self.__execution_id: Optional[str] = None

        self.__std_slots_listener: Optional[Task] = None
        self.__running = False

    async def start(self, workflow: LzyWorkflow) -> str:
        self.__running = True
        self.__workflow = workflow
        client = await self.__get_client()

        default_creds = self.__workflow.owner.storage_registry.default_config()
        exec_id, creds = await client.start_workflow(
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
        return cast(str, exec_id)

    async def exec(
        self,
        calls: List[LzyCall],
        progress: Callable[[ProgressStep], None],
    ) -> None:
        assert self.__execution_id is not None
        assert self.__workflow is not None

        client = await self.__get_client()
        pools = await client.get_pool_specs(self.__execution_id)

        modules: Set[str] = set()
        for call in calls:
            modules.update(cast(Sequence[str], call.env.local_modules_path))

        urls = await self.__load_local_modules(modules)

        _LOG.debug("Building execution graph")
        graph = await asyncio.get_event_loop().run_in_executor(
            None, self.__build_graph, calls, pools, list(zip(modules, urls))
        )  # Running long op in threadpool
        _LOG.debug(f"Starting executing graph {graph}")

        graph_id = await client.execute_graph(self.__execution_id, graph)
        _LOG.debug(f"Requesting remote execution, graph_id={graph_id}")

        progress(ProgressStep.WAITING)
        is_executing = False
        while True:
            await asyncio.sleep(FETCH_STATUS_PERIOD_SEC)
            status = await client.graph_status(self.__execution_id, graph_id)

            if isinstance(status, Executing) and not is_executing:
                is_executing = True
                progress(ProgressStep.EXECUTING)
                continue

            if isinstance(status, Completed):
                progress(ProgressStep.COMPLETED)
                _LOG.debug(f"Graph {graph_id} execution completed")
                break

            if isinstance(status, Failed):
                progress(ProgressStep.FAILED)
                _LOG.debug(f"Graph {graph_id} execution failed: {status.description}")
                raise LzyExecutionException(
                    f"Failed executing graph {graph_id}: {status.description}"
                )

    async def destroy(self):
        client = await self.__get_client()
        try:
            if not self.__running:
                return

            assert self.__execution_id is not None
            assert self.__workflow is not None
            assert self.__std_slots_listener is not None

            await client.finish_workflow(self.__workflow.name, self.__execution_id, "Workflow completed")

            await self.__std_slots_listener  # read all stdout and stderr

            self.__execution_id = None
            self.__workflow = None
            self.__std_slots_listener = None

        finally:
            await client.stop()
            self.__workflow_client = None
            self.__running = False

    async def __load_local_modules(self, module_paths: Iterable[str]) -> Sequence[str]:
        """Returns sequence of urls"""
        assert self.__workflow is not None

        modules_uploaded = []
        for local_module in module_paths:

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
                    "lzy_local_modules",
                    os.path.basename(local_module),
                    fileobj_hash(file),
                )
                file.seek(0)
                client = self.__workflow.owner.storage_registry.default_client()
                if client is None:
                    raise RuntimeError("No default storage client")

                conf = self.__workflow.owner.storage_registry.default_config()
                if conf is None:
                    raise RuntimeError("No default storage config")

                uri = conf.uri + "/" + key
                if not await client.blob_exists(uri):
                    await client.write(uri, file)

                presigned_uri = await client.sign_storage_uri(uri)
                modules_uploaded.append(presigned_uri)

        return modules_uploaded

    @staticmethod
    def __resolve_pool(
        provisioning: Provisioning, pool_specs: Sequence[VmPoolSpec]
    ) -> Optional[VmPoolSpec]:
        provisioning.validate()
        for spec in pool_specs:
            if (
                provisioning.cpu_type == spec.cpuType
                and cast(int, provisioning.cpu_count) <= spec.cpuCount
                and provisioning.gpu_type == spec.gpuType
                and cast(int, provisioning.gpu_count) <= spec.gpuCount
                and cast(int, provisioning.ram_size_gb) <= spec.ramGb
            ):
                return spec
        return None

    async def __get_client(self):
        if self.__workflow_client is None:
            user = os.getenv(USER_ENV)
            key_path = os.getenv(KEY_PATH_ENV)
            if user is None:
                raise ValueError(f"User must be specified by env variable {USER_ENV} or `user` argument")
            if key_path is None:
                raise ValueError(f"Key path must be specified by env variable {KEY_PATH_ENV} or `key_path` argument")
            self.__workflow_client = await WorkflowServiceClient.create(
                os.getenv(ENDPOINT_ENV, "api.lzy.ai:8899"), build_token(user, key_path)
            )
        return self.__workflow_client

    async def __listen_to_std_slots(self, execution_id: str):
        client = await self.__get_client()
        async for data in client.read_std_slots(execution_id):
            if isinstance(data, StdoutMessage):
                system_log = "[SYS]" in data.data
                prefix = COLOURS[get_color()] if system_log else ""
                suffix = RESET_COLOR if system_log else ""
                sys.stdout.write(prefix + data.data + suffix)
            else:
                sys.stderr.write(data.data)

    def __build_graph(
        self,
        calls: List[LzyCall],
        pools: Sequence[VmPoolSpec],
        modules: Sequence[Tuple[str, str]]
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
            ret_descriptions: List[Tuple[Type, str]] = []

            for i, eid in enumerate(call.arg_entry_ids):
                entry = self.__workflow.snapshot.get(eid)
                slot_path = f"/{call.id}/{entry.name}"
                input_slots.append(
                    Operation.SlotDescription(
                        path=slot_path, storageUri=entry.storage_uri
                    )
                )
                arg_descriptions.append((entry.typ, slot_path))

                scheme = entry.data_scheme

                data_descriptions[entry.storage_uri] = DataDescription(
                    storageUri=entry.storage_uri,
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
                slot_path = f"/{call.id}/{entry.name}"
                input_slots.append(
                    Operation.SlotDescription(
                        path=slot_path, storageUri=entry.storage_uri
                    )
                )
                kwarg_descriptions[name] = (entry.typ, slot_path)

                data_descriptions[entry.storage_uri] = DataDescription(
                    storageUri=entry.storage_uri,
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
                entry = self.__workflow.snapshot.get(eid)
                slot_path = f"/{call.id}/{entry.name}"
                output_slots.append(
                    Operation.SlotDescription(
                        path=slot_path, storageUri=entry.storage_uri
                    )
                )

                data_descriptions[entry.storage_uri] = DataDescription(
                    storageUri=entry.storage_uri,
                    dataScheme=DataScheme(
                        dataFormat=entry.data_scheme.data_format,
                        schemeFormat=entry.data_scheme.schema_format,
                        schemeContent=entry.data_scheme.schema_content if entry.data_scheme.schema_content else "",
                        metadata=entry.data_scheme.meta
                    )
                    if entry.data_scheme is not None
                    else None,
                )
                ret_descriptions.append((entry.typ, slot_path))

            pool = self.__resolve_pool(call.provisioning, pools)

            if pool is None:
                raise RuntimeError(
                    f"Cannot resolve pool for operation "
                    f"{call.signature.func.name}:\nAvailable: {pools}\n Expected: {call.provisioning}"
                )
            pool_to_call.append((pool, call))

            docker_image: Optional[str]
            if call.env.docker_image:
                docker_image = call.env.docker_image
            else:
                docker_image = None

            conda_yaml: Optional[str]
            if call.env.conda_yaml_path:
                with open(call.env.conda_yaml_path, "r") as file:
                    conda_yaml = file.read()
            else:
                conda_yaml = generate_conda_yaml(cast(str, call.env.python_version),
                                                 cast(Dict[str, str], call.env.libraries))

            request = ProcessingRequest(
                get_logging_config(),
                serializers=self.__workflow.owner.serializer_registry.imports(),
                op=call.signature.func.callable,
                args_paths=arg_descriptions,
                kwargs_paths=kwarg_descriptions,
                output_paths=ret_descriptions,
                lazy_arguments=call.lazy_arguments
            )

            _com = "".join(
                [
                    "python -u ",  # -u makes stdout/stderr unbuffered. Maybe it should be a parameter
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
                        yaml=conda_yaml,
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
