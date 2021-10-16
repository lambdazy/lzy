import json
import logging
import os
import subprocess
import tempfile
from pathlib import Path
from time import sleep

from lzy.model.channel import Channel, Bindings
from lzy.model.slot import Slot, Direction
from lzy.model.zygote import Zygote
from lzy.servant.servant import Servant


class BashExecutionException(Exception):
    def __init__(self, message, *args):
        super(BashExecutionException, self).__init__(message, *args)
        self.message = message


def exec_bash(*command):
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
        raise BashExecutionException(message=f"Process exited with code {process.returncode}")
    return out


class Singleton(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]


class BashServant(Servant):
    __metaclass__ = Singleton

    def __init__(self):
        super().__init__()
        self._log = logging.getLogger(str(self.__class__))
        self._mount = Path(os.getenv("LZY_MOUNT", default="/tmp/lzy"))
        self._port = os.getenv("LZY_PORT", default=9999)
        self._server_host = os.getenv("LZY_SERVER", default="host.docker.internal")
        self._log.info(f"Creating BashServant at MOUNT_PATH={self._mount}, PORT={self._port}, SERVER={self._server_host}")

    def mount(self) -> Path:
        return self._mount

    def get_slot_path(self, slot: Slot) -> Path:
        return self.mount().joinpath(slot.name().lstrip(os.sep))

    def create_channel(self, channel: Channel):
        self._log.info(f"Creating channel {channel.name}")
        return exec_bash(f"{self._mount}/sbin/channel create " + channel.name)

    def destroy_channel(self, channel: Channel):
        self._log.info(f"Destroying channel {channel.name}")
        return exec_bash(f"{self.mount}/sbin/channel", "destroy", channel.name)

    def touch(self, slot: Slot, channel: Channel):
        self._log.info(f"Creating slot {slot.name()} dir:{slot.direction()} channel:{channel.name}")
        slot_description_file = tempfile.mktemp(prefix="lzy_slot_", suffix=".json", dir="/tmp/")
        with open(slot_description_file, 'w') as f:
            f.write(slot.to_json())
        result = exec_bash(
            f"{self._mount}/sbin/touch",
            str(self.get_slot_path(slot)),
            channel.name,
            "--slot",
            slot_description_file
        )
        if slot.direction() == Direction.OUTPUT:
            while not self.get_slot_path(slot).exists():
                sleep(0.1)
        return result

    def publish(self, zygote: Zygote):
        self._log.info(f"Publishing zygote {zygote.name()}")
        zygote_description_file = tempfile.mktemp(prefix="lzy_zygote_", suffix=".json", dir="/tmp/")
        with open(zygote_description_file, 'w') as f:
            f.write(zygote.to_json())
        return exec_bash(f"{self._mount}/sbin/publish", zygote.name(), zygote_description_file, "-z", self._server_host)

    def run(self, zygote: Zygote, bindings: Bindings):
        self._log.info(f"Running zygote {zygote.name()}")

        zygote_path = f"{self._mount}/bin/{zygote.name()}"
        if not os.path.exists(zygote_path):
            self._log.warning(f"zygote {zygote.name()} was not published")
            self.publish(zygote)

        slots_mapping_file = tempfile.mktemp(prefix="lzy_slot_mapping_", suffix=".json", dir="/tmp/")
        with open(slots_mapping_file, 'w') as f:
            json_bindings = {
                binding.remote_slot.name(): binding.channel.name for binding in bindings.bindings
            }
            json.dump(json_bindings, f, indent=3)
        return exec_bash(zygote_path, "--mapping", slots_mapping_file)
