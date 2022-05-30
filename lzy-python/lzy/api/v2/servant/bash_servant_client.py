import codecs
import json
import logging
import os
import subprocess
import sys
import tempfile
from pathlib import Path
from threading import Thread
from time import sleep
from typing import Any, Dict, Optional, Iterable, List

from lzy.storage.credentials import StorageCredentials, AzureCredentials, AmazonCredentials, AzureSasCredentials
from lzy.api.v2.servant.model.channel import Bindings, Channel, SnapshotChannelSpec
from lzy.api.v2.servant.model.encoding import ENCODING as encoding
from lzy.api.v2.servant.model.execution import Execution, ExecutionResult, ExecutionDescription, InputExecutionValue, \
    ExecutionValue
from lzy.api.v2.servant.model.slot import Slot, Direction
from lzy.api.v2.servant.model.zygote import Zygote
from lzy.api.v2.servant.servant_client import ServantClient, CredentialsTypes


def exec_bash(*command):
    with subprocess.Popen(
            ["bash", "-c", " ".join(command)],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            stdin=subprocess.PIPE
    ) as process:
        out, err = process.communicate()

        if process.returncode != 0:
            raise BashExecutionException(
                message=f"Process exited with code {process.returncode}\n STDERR: " + str(err, encoding)
            )
    return out


class BashExecutionException(Exception):
    def __init__(self, message, *args):
        message += "If you are going to ask for help of cloud support," \
                   " please send the following trace files: /tmp/lzy-log/"
        super().__init__(message, *args)
        self.message = message


class BashExecution(Execution):
    def __init__(
            self, execution_id: str,
            bindings: Bindings, env: Dict[str, str],
            *command
    ):
        super().__init__()
        self._id = execution_id
        self._cmd = command
        self._env = env
        self._bindings = bindings
        self._process: Optional[subprocess.Popen] = None

    def id(self) -> str:
        return self._id

    def bindings(self) -> Bindings:
        return self._bindings

    def start(self) -> None:
        if self._process:
            raise ValueError("Execution has been already started")
        # TODO: make BashExecution a context manager and delegate
        # TODO: __enter__ and __exit__ to __enter__ and __exit__
        # TODO: of self._process. Disabled pylint recommendation for now
        # pylint: disable=consider-using-with
        self._process = subprocess.Popen(
            ["bash", "-c", " ".join(self._cmd)],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            stdin=subprocess.PIPE,
            env=self._env,
        )

    @staticmethod
    def _pipe_to_string(pipe):
        if pipe is None:
            return ""
        return str(pipe, "utf8")

    def write_to_stderr(self):
        dec = codecs.getincrementaldecoder('utf8')()
        for c in iter(lambda: self._process.stderr.read(1), b""):
            sys.stderr.write(dec.decode(c))

    def write_to_stdout(self):
        dec = codecs.getincrementaldecoder('utf8')()
        for c in iter(lambda: self._process.stdout.read(1), b""):
            sys.stdout.write(dec.decode(c))

    def wait_for(self) -> ExecutionResult:
        if not self._process:
            raise ValueError("Execution has NOT been started")
        t1 = Thread(target=self.write_to_stdout)
        t2 = Thread(target=self.write_to_stderr)

        t1.start()
        t2.start()

        t1.join()
        t2.join()
        out, err = self._process.communicate()
        return ExecutionResult(
            BashExecution._pipe_to_string(out),
            BashExecution._pipe_to_string(err),
            self._process.returncode,
        )


class BashServantClient(ServantClient):
    def __init__(self, lzy_mount: Optional[str] = None):
        super().__init__()
        mount_path: str = (
            lzy_mount
            if lzy_mount is not None
            else os.getenv("LZY_MOUNT", default="/tmp/lzy")
        )
        self._mount: Path = Path(mount_path)

        self._log = logging.getLogger(str(self.__class__))
        self._log.info(f"Creating BashServant at MOUNT_PATH={self._mount}")

    def mount(self) -> Path:
        return self._mount

    def get_slot_path(self, slot: Slot) -> Path:
        return self.mount().joinpath(slot.name.lstrip(os.path.sep))

    def create_channel(self, channel: Channel):
        self._log.info(f"Creating channel {channel.name}")
        command = [f"{self.mount()}/sbin/channel", "create", channel.name]
        if isinstance(channel.spec, SnapshotChannelSpec):
            command.extend(["-t", "snapshot", "-s", channel.spec.snapshot_id, "-e", channel.spec.entry_id])
        else:
            command.extend(["-t", "direct"])
        return exec_bash(*command)

    def destroy_channel(self, channel: Channel):
        self._log.info(f"Destroying channel {channel.name}")
        return exec_bash(f"{self.mount()}/sbin/channel", "destroy", channel.name)

    def touch(self, slot: Slot, channel: Channel):
        self._log.info(
            f"Creating slot {slot.name} dir:{slot.direction} channel:{channel.name}"
        )
        slot_description_file = tempfile.mktemp(
            prefix="lzy_slot_", suffix=".json", dir="/tmp/"
        )

        with open(slot_description_file, "w", encoding=encoding) as file:
            file.write(slot.to_json())
        result = exec_bash(
            f"{self.mount()}/sbin/touch",
            str(self.get_slot_path(slot)),
            channel.name,
            "--slot",
            slot_description_file,
        )
        if slot.direction == Direction.OUTPUT:
            while not self.get_slot_path(slot).exists():
                sleep(0.1)
        return result

    def publish(self, zygote: Zygote):
        self._log.info(f"Publishing zygote {zygote.name}")
        zygote_description_file = tempfile.mktemp(
            prefix="lzy_zygote_", suffix=".json", dir="/tmp/"
        )
        with open(zygote_description_file, "w", encoding=encoding) as file:
            file.write(zygote.to_json())
        return exec_bash(
            f"{self.mount()}/sbin/publish", zygote.name, zygote_description_file
        )

    def get_credentials(
            self, typ: CredentialsTypes, bucket: str
    ) -> StorageCredentials:
        self._log.info(f"Getting credentials for {typ}")
        out = exec_bash(f"{self._mount}/sbin/storage", typ.value, bucket)
        data: dict = json.loads(out)
        if "azure" in data:
            return AzureCredentials(data["azure"]["connectionString"])
        if "amazon" in data:
            return AmazonCredentials(
                data["amazon"]["endpoint"],
                data["amazon"]["accessToken"],
                data["amazon"]["secretToken"],
            )

        return AzureSasCredentials(**data["azureSas"])

    def get_bucket(self) -> str:
        self._log.info(f"Getting bucket")
        out = exec_bash(f"{self._mount}/sbin/storage", "bucket")
        data: dict = json.loads(out)
        return str(data["bucket"])

    # pylint: disable=duplicate-code
    def run(self, execution_id: str, zygote: Zygote,
            bindings: Bindings) -> Execution:

        slots_mapping_file = tempfile.mktemp(
            prefix="lzy_slot_mapping_", suffix=".json", dir="/tmp/"
        )
        with open(slots_mapping_file, "w", encoding=encoding) as file:
            json_bindings = {
                binding.slot.name: binding.channel.name
                for binding in bindings
            }
            json.dump(json_bindings, file, indent=3)

        env = os.environ.copy()
        env["ZYGOTE"] = zygote.to_json()

        execution = BashExecution(
            execution_id,
            bindings,
            env,
            f"{self.mount()}/sbin/run",
            "--mapping",
            slots_mapping_file
        )
        execution.start()
        return execution

    def save_execution(self, execution: ExecutionDescription):
        execution_description_file = tempfile.mktemp(
            prefix="lzy_execution_description_", suffix=".json", dir="/tmp/"
        )
        with open(execution_description_file, "w", encoding=encoding) as file:
            json.dump({
                "description": {
                    "name": execution.name,
                    "snapshotId": execution.snapshot_id,
                    "input": [
                        {
                            "name": val.name,
                            "hash": val.hash,
                            "entryId": val.entry_id
                        }
                        for val in execution.inputs
                    ],
                    "output": [
                        {
                            "name": val.name,
                            "entryId": val.entry_id
                        }
                        for val in execution.outputs
                    ]
                }
            }, file)
        exec_bash(f"{self._mount}/sbin/cache", "save", execution_description_file)

    def resolve_executions(self, name: str,
                           snapshot_id: str, inputs: Iterable[InputExecutionValue]) -> List[ExecutionDescription]:
        execution_description_file = tempfile.mktemp(
            prefix="lzy_execution_description_", suffix=".json", dir="/tmp/"
        )
        with open(execution_description_file, "w", encoding=encoding) as file:
            args = []
            for val in inputs:
                if val.hash:
                    args.append({
                        "name": val.name,
                        "hash": val.hash
                    })
                else:
                    args.append({
                        "name": val.name,
                        "entryId": str(val.entry_id)
                    })

            description = {
                "operationName": name,
                "snapshotId": snapshot_id,
                "args": args
            }
            json.dump(description, file)

        ret = exec_bash(f"{self._mount}/sbin/cache", "find", execution_description_file)
        return [
            ExecutionDescription(exec_description['name'], exec_description['snapshotId'], [
                InputExecutionValue(
                    val['name'],
                    val['entryId'],
                    val['hash']
                ) for val in exec_description.get('input', [])
            ], [
                ExecutionValue(
                    val['name'],
                    val['entryId']
                ) for val in exec_description.get('output', [])
            ])
            for exec_description in json.loads(ret).get("execution", [])
        ]
