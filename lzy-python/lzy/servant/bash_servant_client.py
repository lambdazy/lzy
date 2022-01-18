import json
import logging
import os
import subprocess
import tempfile
from pathlib import Path
from typing import Any, Dict, Optional, Mapping

import sys
from time import sleep

from lzy.api.whiteboard.credentials import AzureCredentials, AmazonCredentials, StorageCredentials
from lzy.model.channel import Channel, Bindings
from lzy.model.slot import Slot, Direction
from lzy.model.zygote import Zygote
from lzy.servant.servant_client import ServantClient, Execution, ExecutionResult


class BashExecutionException(Exception):
    def __init__(self, message, *args):
        super(BashExecutionException, self).__init__(message, *args)
        self.message = message


class Singleton(type):
    _instances: Dict[type, Any] = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            # noinspection PyArgumentList
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]


class BashExecution(Execution):
    def __init__(self, execution_id: str, bindings: Bindings,
                 env: Dict[str, str], *command):
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
            raise ValueError('Execution has been already started')
        self._process = subprocess.Popen(
            ["bash", "-c", " ".join(self._cmd)],
            stdout=sys.stdout,
            stderr=sys.stderr,
            stdin=subprocess.PIPE,
            env=self._env
        )

    @staticmethod
    def _pipe_to_string(pipe):
        if pipe is None:
            return ""
        return str(pipe, "utf8")

    def wait_for(self) -> ExecutionResult:
        if not self._process:
            raise ValueError('Execution has NOT been started')
        out, err = self._process.communicate()
        return ExecutionResult(
            BashExecution._pipe_to_string(out),
            BashExecution._pipe_to_string(err),
            self._process.returncode
        )


class BashServantClient(ServantClient):
    _instance: Optional['BashServantClient'] = None

    def __init__(self, lzy_mount: Optional[str] = None):
        super().__init__()
        mount_path: str = lzy_mount if lzy_mount is not None \
            else os.getenv("LZY_MOUNT", default="/tmp/lzy")
        self._mount: Path = Path(mount_path)

        self._log = logging.getLogger(str(self.__class__))
        self._log.info(f"Creating BashServant at MOUNT_PATH={self._mount}")

    def mount(self) -> Path:
        return self._mount

    def get_slot_path(self, slot: Slot) -> Path:
        return self.mount().joinpath(slot.name.lstrip(os.path.sep))

    def create_channel(self, channel: Channel):
        self._log.info(f"Creating channel {channel.name}")
        return self._exec_bash(f"{self.mount()}/sbin/channel create " + channel.name)

    def destroy_channel(self, channel: Channel):
        self._log.info(f"Destroying channel {channel.name}")
        return self._exec_bash(f"{self.mount()}/sbin/channel", "destroy", channel.name)

    def touch(self, slot: Slot, channel: Channel):
        self._log.info(f"Creating slot {slot.name} dir:{slot.direction} channel:{channel.name}")
        slot_description_file = tempfile.mktemp(prefix="lzy_slot_", suffix=".json", dir="/tmp/")

        with open(slot_description_file, 'w') as f:
            f.write(slot.to_json())
        result = self._exec_bash(
            f"{self.mount()}/sbin/touch",
            str(self.get_slot_path(slot)),
            channel.name,
            "--slot",
            slot_description_file
        )
        if slot.direction == Direction.OUTPUT:
            while not self.get_slot_path(slot).exists():
                sleep(0.1)
        return result

    def publish(self, zygote: Zygote):
        self._log.info(f"Publishing zygote {zygote.name}")
        zygote_description_file = tempfile.mktemp(prefix="lzy_zygote_",
                                                  suffix=".json", dir="/tmp/")
        with open(zygote_description_file, 'w') as f:
            f.write(zygote.to_json())
        return self._exec_bash(f"{self.mount()}/sbin/publish", zygote.name,
                               zygote_description_file)

    def get_credentials(self, typ: ServantClient.CredentialsTypes) -> StorageCredentials:
        self._log.info(f"Getting credentials for {typ}")
        out = self._exec_bash(f"{self._mount}/sbin/credentials", typ.value)
        data: dict = json.loads(out)
        if "azure" in data:
            return AzureCredentials(data["azure"]["connectionString"])
        return AmazonCredentials(data["amazon"]["endpoint"], data["amazon"]["accessToken"],
                                 data["amazon"]["secretToken"])

    def run(self, execution_id: str, zygote: Zygote,
            bindings: Bindings, entry_id_mapping: Optional[Mapping[Slot, str]]) -> Execution:
        slots_mapping_file = tempfile.mktemp(prefix="lzy_slot_mapping_",
                                             suffix=".json", dir="/tmp/")
        entry_id_mapping_file = tempfile.mktemp(prefix="entry_id_mapping_",
                                                suffix=".json", dir="/tmp/")
        with open(slots_mapping_file, 'w') as f:
            json_bindings = {
                binding.remote_slot.name: binding.channel.name for binding in
                bindings.bindings()
            }
            json.dump(json_bindings, f, indent=3)
        with open(entry_id_mapping_file, 'w') as f:
            if entry_id_mapping:
                json_bindings = {
                    slot.name: entry_id for slot, entry_id in
                    entry_id_mapping.items()
                }
            else:
                json_bindings = {}
            json.dump(json_bindings, f, indent=3)

        env = os.environ.copy()
        env['ZYGOTE'] = zygote.to_json()

        execution = BashExecution(execution_id, bindings, env,
                                  f"{self.mount()}/sbin/run", "--mapping",
                                  slots_mapping_file,
                                  "-s", entry_id_mapping_file)
        execution.start()
        return execution

    @staticmethod
    def _exec_bash(*command):
        process = subprocess.Popen(
            ["bash", "-c", " ".join(command)],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            stdin=subprocess.PIPE
        )
        out, err = process.communicate()
        if err != b'':
            raise BashExecutionException(message=str(err, encoding='utf-8'))
        if process.returncode != 0:
            raise BashExecutionException(
                message=f"Process exited with code {process.returncode}")
        return out

    @classmethod
    def instance(cls, lzy_mount: Optional[str] = None) -> 'BashServantClient':
        if cls._instance is None:
            cls._instance = BashServantClient(lzy_mount)
        return cls._instance
