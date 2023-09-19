from __future__ import annotations

import asyncio
import functools
import os
import sys
import tempfile
import uuid
from asyncio import Task
from io import BytesIO
from typing import (
    TYPE_CHECKING,
    BinaryIO,
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

from lzy.types import NamedVmSpec
from lzy.proxy.result import Result
from lzy.env.container.docker import DockerPullPolicy, DockerContainer
from lzy.env.container.no_container import NoContainer
from lzy.api.v1.snapshot import SnapshotEntry
from lzy.api.v1.exceptions import LzyExecutionException
from lzy.api.v1.remote.lzy_service_client import (
    Completed,
    Executing,
    Failed,
    StderrMessage,
    LzyServiceClient,
)
from lzy.api.v1.runtime import (
    ProgressStep,
    Runtime,
)
from lzy.api.v1.startup import ProcessingRequest
from lzy.api.v1.utils.pickle import pickle
from lzy.logs.config import get_logger, get_logging_config, RESET_COLOR, COLOURS, get_syslog_color
from lzy.storage.api import Storage, FSCredentials
from lzy.utils.grpc import retry, RetryConfig
from lzy.utils.files import fileobj_hash_str, zip_path

if TYPE_CHECKING:
    from lzy.core.call import LzyCall
    from lzy.core.workflow import LzyWorkflow


FETCH_STATUS_PERIOD_SEC = float(os.getenv("FETCH_STATUS_PERIOD_SEC", "10"))

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


def data_description_by_entry(entry: SnapshotEntry) -> DataDescription:
    data_scheme = None
    if entry.data_scheme is not None:
        data_scheme = DataScheme(
            dataFormat=entry.data_scheme.data_format,
            schemeFormat=entry.data_scheme.schema_format,
            schemeContent=entry.data_scheme.schema_content if entry.data_scheme.schema_content else "",
            metadata=entry.data_scheme.meta
        )

    return DataDescription(
        storageUri=entry.storage_uri,
        dataScheme=data_scheme
    )


class RemoteRuntime(Runtime):
    def __init__(self) -> None:
        self.__lzy_client: LzyServiceClient = LzyServiceClient()
        self.__storage: Optional[Storage] = None

        self.__workflow: Optional[LzyWorkflow] = None
        self.__execution_id: Optional[str] = None

        self.__std_slots_listener: Optional[Task] = None
        self.__running = False

        self.__logs_offset = 0  # Logs offset for retries of reading log data

    async def storage(self) -> Optional[Storage]:
        if not self.__storage:
            self.__storage = await self.__lzy_client.get_or_create_storage(idempotency_key=self.__gen_rand_idempt_key())
        return self.__storage

    @staticmethod
    def __gen_rand_idempt_key() -> str:
        return str(uuid.uuid4())

    async def start(self, workflow: LzyWorkflow) -> str:
        storage = workflow.owner.storage_registry.default_config()
        storage_name = workflow.owner.storage_registry.default_storage_name()
        if storage is None or storage_name is None:
            raise ValueError("No provided storage")
        if isinstance(storage.credentials, FSCredentials):
            raise ValueError("Local FS storage cannot be default for remote runtime")

        _LOG.debug(f"Starting workflow {workflow.name} with storage {storage}")

        exec_id = await self.__lzy_client.start_workflow(workflow_name=workflow.name, storage=storage,
                                                         storage_name=storage_name,
                                                         idempotency_key=self.__gen_rand_idempt_key())
        self.__running = True
        self.__workflow = workflow
        self.__execution_id = exec_id
        self.__std_slots_listener = asyncio.create_task(self.__listen_to_std_slots(exec_id))
        self.__logs_offset = 0
        return cast(str, exec_id)

    async def exec(
        self,
        calls: List[LzyCall],
        progress: Callable[[ProgressStep], None],
    ) -> None:
        if not self.__running:
            raise ValueError("Runtime is not running")

        client = self.__lzy_client
        assert self.__workflow
        workflow = self.__workflow
        pools = await client.get_pool_specs(workflow_name=workflow.name, execution_id=self.__execution_id)

        modules: Set[str] = set()

        for call in calls:
            modules.update(call.get_local_module_paths())

        urls = await self.__load_local_modules(modules)

        _LOG.debug("Building execution graph")
        graph = await asyncio.get_event_loop().run_in_executor(
            None, self.__build_graph, calls, pools, list(zip(modules, urls))
        )  # Running long op in threadpool
        _LOG.debug(f"Starting executing graph {graph}")

        graph_id = await client.execute_graph(workflow_name=workflow.name, execution_id=self.__execution_id,
                                              graph=graph, idempotency_key=self.__gen_rand_idempt_key())
        if not graph_id:
            _LOG.debug("Results of all graph operations are cached. Execution graph is not started")
            return

        _LOG.debug(f"Requesting remote execution, graph_id={graph_id}")

        progress(ProgressStep.WAITING)
        is_executing = False
        while True:
            await asyncio.sleep(FETCH_STATUS_PERIOD_SEC)
            status = await client.graph_status(workflow_name=workflow.name, execution_id=self.__execution_id,
                                               graph_id=graph_id)

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
                for call in self.__workflow.call_queue:
                    if call.signature.func.name == status.failed_task_name:
                        workflow = self.__workflow
                        exception = await workflow.snapshot.get_data(call.exception_id)
                        if isinstance(exception, Result):
                            exc_typ, exc_value, exc_trace = exception.value
                            raise exc_value.with_traceback(exc_trace)
                raise LzyExecutionException(
                    f"Failed executing graph {graph_id}: {status.description}"
                )

    async def abort(self) -> None:
        client = self.__lzy_client
        if not self.__running:
            return

        assert self.__workflow
        workflow = self.__workflow
        try:
            await client.abort_workflow(workflow_name=workflow.name, execution_id=self.__execution_id,
                                        reason="Workflow execution aborted",
                                        idempotency_key=self.__gen_rand_idempt_key())
            try:
                if self.__std_slots_listener is not None:
                    await asyncio.wait_for(self.__std_slots_listener, timeout=1)
            except asyncio.TimeoutError:
                _LOG.warning("Cannot wait for end of std logs of execution.")
        finally:
            self.__running = False
            self.__execution_id = None
            self.__workflow = None
            self.__std_slots_listener = None

    async def finish(self):
        client = self.__lzy_client
        if not self.__running:
            return
        assert self.__workflow
        workflow = self.__workflow
        try:
            await client.finish_workflow(workflow_name=workflow.name, execution_id=self.__execution_id,
                                         reason="Workflow completed", idempotency_key=self.__gen_rand_idempt_key())
            try:
                if self.__std_slots_listener is not None:
                    await asyncio.wait_for(self.__std_slots_listener, timeout=1)
            except asyncio.TimeoutError:
                _LOG.warning("Cannot wait for end of std logs of execution.")
        finally:
            self.__running = False
            self.__execution_id = None
            self.__workflow = None
            self.__std_slots_listener = None

    async def __load_local_modules(self, module_paths: Iterable[str]) -> Sequence[str]:
        """Returns sequence of urls"""
        assert self.__workflow is not None

        modules_uploaded = []
        for local_module in module_paths:

            with tempfile.NamedTemporaryFile("rb") as archive:
                zip_path(local_module, cast(BinaryIO, archive))

                file = cast(BytesIO, archive.file)
                key = os.path.join(
                    "lzy_local_modules",
                    os.path.basename(local_module),
                    fileobj_hash_str(file),
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

    @retry(action_name="listening to std slots", config=RetryConfig(max_retry=12000, backoff_multiplier=1.2))
    async def __listen_to_std_slots(self, execution_id: str):
        client = self.__lzy_client
        assert self.__workflow
        workflow = self.__workflow
        async for msg in client.read_std_slots(
            workflow_name=workflow.name, execution_id=execution_id, logs_offset=self.__logs_offset
        ):
            task_id_prefix = COLOURS["WHITE"] + "[LZY-REMOTE-" + msg.task_id + "] " + RESET_COLOR
            if isinstance(msg, StderrMessage):
                system_log = "[SYS]" in msg.message
                prefix = COLOURS[get_syslog_color()] if system_log else ""
                suffix = RESET_COLOR if system_log else ""
                sys.stderr.write(task_id_prefix + prefix + msg.message + suffix + '\n')
            else:
                sys.stdout.write(task_id_prefix + msg.message + '\n')

            self.__logs_offset = max(self.__logs_offset, msg.offset)

        sys.stdout.flush()
        sys.stderr.flush()

    def __build_graph(
        self,
        calls: List[LzyCall],
        pools: Sequence[VmPoolSpec],
        modules: Sequence[Tuple[str, str]]
    ) -> Graph:
        assert self.__workflow is not None

        operations: List[Operation] = []
        data_descriptions: Dict[str, DataDescription] = {}
        pool_to_call: List[Tuple[NamedVmSpec, LzyCall]] = []

        for call in calls:
            input_slots: List[Operation.SlotDescription] = []
            output_slots: List[Operation.SlotDescription] = []
            arg_descriptions: List[Tuple[Type, str]] = []
            kwarg_descriptions: Dict[str, Tuple[Type, str]] = {}
            ret_descriptions: List[Tuple[Type, str]] = []

            for eid in call.arg_entry_ids:
                entry = self.__workflow.snapshot.get(eid)
                slot_path = f"/{call.id}/{entry.id}"
                input_slots.append(Operation.SlotDescription(path=slot_path, storageUri=entry.storage_uri))
                arg_descriptions.append((entry.typ, slot_path))

                data_descriptions[entry.storage_uri] = data_description_by_entry(entry)

            for name, eid in call.kwarg_entry_ids.items():
                entry = self.__workflow.snapshot.get(eid)
                slot_path = f"/{call.id}/{entry.id}"
                input_slots.append(Operation.SlotDescription(path=slot_path, storageUri=entry.storage_uri))
                kwarg_descriptions[name] = (entry.typ, slot_path)

                data_descriptions[entry.storage_uri] = data_description_by_entry(entry)

            for eid in call.entry_ids:
                entry = self.__workflow.snapshot.get(eid)
                slot_path = f"/{call.id}/{entry.id}"
                output_slots.append(Operation.SlotDescription(path=slot_path, storageUri=entry.storage_uri))

                data_descriptions[entry.storage_uri] = data_description_by_entry(entry)
                ret_descriptions.append((entry.typ, slot_path))

            exc_entry = self.__workflow.snapshot.get(call.exception_id)
            exc_slot_path = f"/{call.id}/{exc_entry.id}"
            output_slots.append(Operation.SlotDescription(path=exc_slot_path, storageUri=exc_entry.storage_uri))
            data_descriptions[exc_entry.storage_uri] = data_description_by_entry(exc_entry)
            exc_description: Tuple[Type, str] = (exc_entry.typ, exc_slot_path)

            vm_spec_pools = [NamedVmSpec.from_proto(proto) for proto in pools]
            pool = call.get_provisioning().resolve_pool(vm_spec_pools)
            pool_to_call.append((pool, call))

            conda_yaml = call.get_conda_yaml()

            python_env = Operation.PythonEnvSpec(
                yaml=conda_yaml,
                localModules=[
                    Operation.PythonEnvSpec.LocalModule(name=name, url=url)
                    for (name, url) in modules
                ],
            )

            request = ProcessingRequest(
                get_logging_config(),
                serializers=self.__workflow.owner.serializer_registry.imports(),
                op=call.signature.func.callable,
                args_paths=arg_descriptions,
                kwargs_paths=kwarg_descriptions,
                output_paths=ret_descriptions,
                exception_path=exc_description,
                lazy_arguments=call.lazy_arguments
            )
            pickled_request = pickle(request)

            command = " ".join([
                "python -u",  # -u makes stdout/stderr unbuffered. Maybe it should be a parameter
                "$(python -c 'import lzy.api.v1.startup as startup; print(startup.__file__)')",
                pickled_request,
            ])

            container = call.get_container()

            docker_image: Optional[str] = None
            docker_credentials: Optional[Operation.DockerCredentials] = None
            docker_pull_policy = Operation.ALWAYS
            if isinstance(container, DockerContainer):
                docker_credentials = Operation.DockerCredentials(
                    registryName=container.get_registry(),
                    username=container.get_username() or "",
                    password=container.get_password() or "",
                )
                docker_image = container.get_image()
                docker_pull_policy = (
                    Operation.ALWAYS
                    if container.get_pull_policy() == DockerPullPolicy.ALWAYS else
                    Operation.IF_NOT_EXISTS
                )
            elif not isinstance(container, NoContainer):
                raise TypeError(f'unknown type of container {container!r}')

            operations.append(
                Operation(
                    name=call.signature.func.name,
                    description=call.description,
                    inputSlots=input_slots,
                    outputSlots=output_slots,
                    command=command,
                    env=call.get_env_vars(),
                    dockerImage=docker_image or "",
                    dockerCredentials=docker_credentials,
                    dockerPullPolicy=docker_pull_policy,
                    python=python_env,
                    poolSpecName=pool.name,
                )
            )

            _LOG.debug('final env for op {call.callable_name}: {call.final_env}')

        if self.__workflow.interactive:  # TODO(artolord) add costs
            s = ""
            for pool, call in pool_to_call:
                s += f"Call to op {call.callable_name} mapped to pool {pool}\n"

            s += "Are you sure you want to run the graph with this configuration?"

            print(s)
            s = input("(Yes/[No]): ")
            if s.lower() not in ("yes", "y"):
                raise RuntimeError("Graph execution cancelled")

        return Graph(
            name="",
            dataDescriptions=list(data_descriptions.values()),
            operations=operations,
            zone="",
        )
