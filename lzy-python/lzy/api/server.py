import site
import subprocess
import sys
from abc import ABC
from pathlib import Path
import uuid
import base64


# mb dataclass
class Channel:
    def __init__(self, id, input, output):
        self.input = Path(input)
        self.output = Path(output)
        self.id = id


class FileChannel(Channel):
    def __init__(self, id, filepath):
        super().__init__(id, filepath, filepath)


class Server:
    def __init__(self, tmp_path):
        self._path = Path(tmp_path)
        self._processes = []
        self._channels = []
        self._channels_file = self._make_tmp()

    # comand should be bytes, starts zygote
    def publish(self, comand, mapping):
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
        id = len(self._channels)
        with self._channels_file.open(mode='a') as chnl:
            chnl.write(f'{id}: {path.absolute()}\n')
        self._channels.append(FileChannel(id, path))
        return self._channels[-1]

    def _make_tmp(self):
        return self._path.joinpath(str(uuid.uuid4()))

    def release_file(self):
        pass


server = Server('./tmp')
