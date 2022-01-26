import logging
import os
import subprocess
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from lzy.model.encoding import ENCODING as encoding


@dataclass
class TerminalConfig:
    private_key_path: str = "~/.ssh/id_rsa"
    server_url: str = "api.lzy.ai:8899"
    lzy_mount: str = ""
    user: Optional[str] = None
    yaml_path: Optional[str] = None

    def __post_init__(self):
        if not self.lzy_mount:
            self.lzy_mount = os.getenv("LZY_MOUNT", default="/tmp/lzy")


class TerminalServer:
    jar_path = Path(os.path.dirname(__file__)) / ".." / "lzy-servant.jar"
    jar_path = jar_path.resolve().absolute()
    start_timeout_sec = 30

    def __init__(
        self,
        config: TerminalConfig,
        custom_log_file: str = "./custom_terminal_log",
        terminal_log_path: str = "./terminal_log",
    ):
        self._config = config
        self._log_file = custom_log_file
        self._terminal_log_path = terminal_log_path
        self._terminal_log = None
        self._pcs = None
        self._already_started = False
        self._log = logging.getLogger(str(self.__class__))

    def start(self):
        sbin_channel = Path(self._config.lzy_mount) / "sbin" / "channel"
        # using safe version instead of `exists()` because path can be not mounted
        self._already_started = self._check_exists_safe(sbin_channel)
        if self._already_started:
            self._log.info("Using already started servant")
            return

        private_key_path = Path(self._config.private_key_path).expanduser()
        if not private_key_path.resolve().exists():
            raise ValueError("Private key path does not exists: "
                             f"{self._config.private_key_path}")

        # TODO: understand why terminal writes to stdout even with
        # TODO: custom.log.file argument and drop terminal_log_path and
        # TODO: redirection
        if not self._terminal_log:
            # pylint: disable=consider-using-with
            self._terminal_log = open(self._terminal_log_path,
                                      "w", encoding=encoding)
        env = os.environ.copy()
        env["USER"] = self._config.user
        # pylint: disable=consider-using-with
        self._pcs = subprocess.Popen(
            [
                "java",
                "-Dfile.encoding=UTF-8",
                "-Djava.util.concurrent.ForkJoinPool.common.parallelism=32",
                "-Djava.library.path=/usr/local/lib",
                f"-Dcustom.log.file={self._log_file}",
                "-classpath",
                TerminalServer.jar_path,
                "ru.yandex.cloud.ml.platform.lzy.servant.BashApi",
                "--lzy-address",
                self._config.server_url,
                "--lzy-mount",
                self._config.lzy_mount,
                "--private-key",
                self._config.private_key_path,
                "--host",
                "localhost",
                "terminal",
            ],
            stdout=self._terminal_log,
            stderr=self._terminal_log,
            env=env,
        )
        started_ts = int(time.time())
        while not self._check_exists_safe(sbin_channel) \
                and int(time.time()) < started_ts + self.start_timeout_sec:
            time.sleep(0.2)

        if not self._check_exists_safe(sbin_channel):
            raise ValueError("Lzy terminal failed to start")

    def stop(self):
        if not self._already_started:
            assert self._pcs is not None, "Terminal hasn't been started"
            self._pcs.kill()
            self._pcs.wait()
            if self._terminal_log:
                self._terminal_log.flush()
                self._terminal_log.close()

    @staticmethod
    def _check_exists_safe(path: Path) -> bool:
        try:
            return path.exists()
        except OSError: # TODO: find out what should be here
            return False
