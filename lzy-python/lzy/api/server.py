import site
import subprocess
import sys
from pathlib import Path
from typing import Any, List, Mapping
import uuid
import base64


# mb dataclass
class Channel:
    def __init__(self, _id: int, _input: Path, output: Path):
        self.input = Path(_input)
        self.output = Path(output)
        self.id = _id


class FileChannel(Channel):
    def __init__(self, _id: int, filepath: Path):
        super().__init__(_id, filepath, filepath)


class Server:
    def __init__(self, tmp_path: str):
        self._path = Path(tmp_path)
        self._processes: List[Any] = []
        self._channels: List[FileChannel] = []
        self._channels_file = self._make_tmp()

    # comand should be bytes, starts zygote
    def publish(self, comand: bytes, mapping: Mapping):
        map_args = []
        for k, v in mapping.items():
            map_args.append(str(k))
            map_args.append(str(v))

        process = subprocess.Popen(
            [sys.executable,
             site.getsitepackages()[0] + "/lzy/startup.py",
             base64.b64encode(comand),
             self._channels_file,  # for getting all available channels
             *map_args,
             ]
        )
        return process

    def create_channel(self) -> Channel:
        path = self._make_tmp()
        path.open(mode='wb').close()

        # file which will be used for simulating list channel request
        # from servant to server
        _id = len(self._channels)
        with self._channels_file.open(mode='a') as chnl:
            chnl.write(f'{_id}: {path.absolute()}\n')
        self._channels.append(FileChannel(_id, path))
        return self._channels[-1]

    def _make_tmp(self):
        return self._path.joinpath(str(uuid.uuid4()))

    def release_file(self):
        pass


server = Server('./tmp')
