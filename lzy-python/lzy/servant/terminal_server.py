import logging
import os
import subprocess
import time
from pathlib import Path


class TerminalServer:
    jar_path = Path(os.path.dirname(__file__)) / '..' / 'lzy-servant.jar'
    jar_path = jar_path.resolve().absolute()
    start_timeout_sec = 30

    def __init__(self, private_key_path: str, lzy_mount: str, url: str, user: str,
                 custom_log_file: str = './custom_terminal_log',
                 terminal_log_path: str = './terminal_log'):
        self._private_key = private_key_path
        self._lzy_mount = lzy_mount
        self._user = user
        self._url = url
        self._log_file = custom_log_file
        self._terminal_log_path = terminal_log_path
        self._terminal_log = None
        self._pcs = None
        self._already_started = False
        self._log = logging.getLogger(str(self.__class__))

    def start(self):
        sbin_channel = Path(self._lzy_mount) / 'sbin' / 'channel'
        # using safe version instead of `exists()` because path can be not mounted
        self._already_started = self._check_exists_safe(sbin_channel)
        if self._already_started:
            self._log.info("Using already started servant")
            return

        if not os.path.exists(os.path.expanduser(self._private_key)):
            raise ValueError(f'Private key path does not exists: {self._private_key}')

        # TODO: understand why terminal writes to stdout even with
        # TODO: custom.log.file argument and drop terminal_log_path and
        # TODO: redirection
        if not self._terminal_log:
            self._terminal_log = open(self._terminal_log_path, 'w')
        env = os.environ.copy()
        env['USER'] = self._user
        self._pcs = subprocess.Popen(
            ['java', '-Dfile.encoding=UTF-8',
             '-Djava.util.concurrent.ForkJoinPool.common.parallelism=32',
             '-Djava.library.path=/usr/local/lib',
             f'-Dcustom.log.file={self._log_file}',
             '-classpath', TerminalServer.jar_path,
             'ru.yandex.cloud.ml.platform.lzy.servant.BashApi',
             '--lzy-address', self._url,
             '--lzy-mount', self._lzy_mount,
             '--private-key', self._private_key,
             '--host', 'localhost',
             'terminal'
             ], stdout=self._terminal_log, stderr=self._terminal_log, env=env)
        started_ts = int(time.time())
        while not self._check_exists_safe(sbin_channel) and int(time.time()) < started_ts + self.start_timeout_sec:
            time.sleep(0.2)
        if not self._check_exists_safe(sbin_channel):
            raise ValueError('Lzy terminal failed to start')

    def stop(self):
        if not self._already_started:
            assert self._pcs is not None, "Terminal hasn't been started"
            self._pcs.kill()
            if self._terminal_log:
                self._terminal_log.flush()
                self._terminal_log.close()

    @staticmethod
    def _check_exists_safe(path: Path) -> bool:
        try:
            return path.exists()
        except Exception:
            return False
