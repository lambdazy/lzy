import asyncio
import os
import subprocess
import sys
import tempfile
import uuid
from collections import defaultdict
from pathlib import Path
from typing import TYPE_CHECKING, Callable, Dict, List, Optional, Tuple, Type, cast, IO

from lzy.api.v1.exceptions import LzyExecutionException
from lzy.api.v1.startup import ProcessingRequest
from lzy.api.v1.utils.pickle import pickle
from lzy.logs.config import get_logging_config, COLOURS, get_color, RESET_COLOR
from lzy.storage.api import AsyncStorageClient, Storage

if TYPE_CHECKING:
    from lzy.api.v1 import LzyWorkflow

from lzy.api.v1.call import LzyCall
from lzy.api.v1.runtime import (
    ProgressStep,
    Runtime,
)


class LocalRuntime(Runtime):
    def __init__(self, file_storage_uri: str = "file:///tmp/lzy_storage"):
        self.__workflow: Optional["LzyWorkflow"] = None
        self.__storage: Storage = Storage.fs_storage(file_storage_uri)

    async def storage(self) -> Optional[Storage]:
        return self.__storage

    async def start(self, workflow: "LzyWorkflow") -> str:
        self.__workflow = workflow
        return str(uuid.uuid4())

    async def exec(
        self,
        calls: List[LzyCall],
        progress: Callable[[ProgressStep], None],
    ):
        assert self.__workflow is not None
        graph: Dict[str, List[LzyCall]] = defaultdict(list)
        eid_to_call: Dict[str, LzyCall] = {}
        used: Dict[str, bool] = defaultdict(lambda: False)

        for call in calls:
            for eid in call.arg_entry_ids:
                graph[eid].append(call)

            for eid in call.kwarg_entry_ids.values():
                graph[eid].append(call)

            for eid in call.entry_ids:
                eid_to_call[eid] = call

        ans: List[str] = []

        # noinspection PyShadowingNames
        def dfs(eid: str):
            for c in graph[eid]:
                for edge in c.entry_ids:
                    if not used[edge]:
                        used[edge] = True
                        dfs(edge)
            ans.append(eid)

        for eid in eid_to_call.keys():
            if not used[eid]:
                dfs(eid)

        ans.reverse()
        mat: Dict[str, bool] = defaultdict(lambda: False)

        for eid in ans:
            c = eid_to_call[eid]
            if mat[c.id]:
                continue
            await self.__exec_call(c)
            mat[c.id] = True

    async def __exec_call(self, call: LzyCall):
        assert self.__workflow is not None

        arg_descriptions: List[Tuple[Type, str]] = []
        kwarg_descriptions: Dict[str, Tuple[Type, str]] = {}
        ret_descriptions: List[Tuple[Type, str]] = []

        with tempfile.TemporaryDirectory("_lzy_call") as folder:
            args_read = []
            for eid in call.arg_entry_ids:
                entry = self.__workflow.snapshot.get(eid)
                entry_path = folder + "/" + eid
                args_read.append(self.__from_storage_to_file(entry.storage_uri, entry_path))
                arg_descriptions.append((entry.typ, entry_path[len(folder):]))
            await asyncio.gather(*args_read)

            kwargs_read = []
            for name, eid in call.kwarg_entry_ids.items():
                entry = self.__workflow.snapshot.get(eid)
                entry_path = folder + "/" + eid
                kwargs_read.append(self.__from_storage_to_file(entry.storage_uri, entry_path))
                kwarg_descriptions[name] = (entry.typ, entry_path[len(folder):])
            await asyncio.gather(*kwargs_read)

            for i, eid in enumerate(call.entry_ids):
                path = Path(folder + "/" + eid)
                path.touch()
                entry = self.__workflow.snapshot.get(eid)
                ret_descriptions.append((entry.typ, str(path)[len(folder):]))

            request = ProcessingRequest(
                get_logging_config(),
                serializers=self.__workflow.owner.serializer_registry.imports(),
                op=call.signature.func.callable,
                args_paths=arg_descriptions,
                kwargs_paths=kwarg_descriptions,
                output_paths=ret_descriptions,
                lazy_arguments=call.lazy_arguments
            )

            directory = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
            command = [
                "python",
                "-u",
                directory + "/startup.py",
                pickle(request)
            ]

            env_vars = os.environ.copy()
            env_vars["LZY_MOUNT"] = folder
            main_module = sys.modules['__main__']
            if hasattr(main_module, '__file__'):
                main_dir_path = cast(str, os.path.dirname(cast(str, getattr(main_module, '__file__'))))
                if "PYTHONPATH" in env_vars:
                    env_vars["PYTHONPATH"] = f"{env_vars['PYTHONPATH']}:{main_dir_path}"
                else:
                    env_vars["PYTHONPATH"] = main_dir_path
            result = subprocess.Popen(command, env=env_vars, stdout=subprocess.PIPE)
            out = cast(IO[bytes], result.stdout)
            for line in iter(out.readline, b''):
                str_line = line.decode("utf-8")
                system_log = "[SYS]" in str_line
                prefix = COLOURS[get_color()] if system_log else ""
                suffix = RESET_COLOR if system_log else ""
                sys.stdout.write(prefix + str_line + suffix)
            out.close()

            rc = result.wait()
            if rc != 0:
                raise LzyExecutionException(f"Error during execution of {call.signature.func.callable}")

            data_to_put = []
            for i, eid in enumerate(call.entry_ids):
                entry = self.__workflow.snapshot.get(eid)
                data_to_put.append(self.__from_file_to_storage(entry.storage_uri, folder + "/" + eid))
            await asyncio.gather(*data_to_put)

    async def abort(self) -> None:
        pass

    async def finish(self) -> None:
        pass

    async def __from_storage_to_file(self, url: str, path: str) -> None:
        with open(path, "wb+") as file:
            await cast(AsyncStorageClient,
                       cast("LzyWorkflow", self.__workflow).owner.storage_registry.default_client()).read(url, file)

    async def __from_file_to_storage(self, url: str, path: str) -> None:
        with open(path, "rb") as file:
            await cast(AsyncStorageClient,
                       cast("LzyWorkflow", self.__workflow).owner.storage_registry.default_client()).write(url, file)
