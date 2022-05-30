import logging
import os
import subprocess
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from lzy.api.v2.servant.model.encoding import ENCODING as encoding
# noinspection PyUnresolvedReferences
import lzy.api  # needed to instantiate logging #  pylint: disable=unused-import


@dataclass
class TerminalConfig:
    server_url: str = "api.lzy.ai:8899"
    port: int = 9999
    lzy_mount: str = ""
    debug_port: int = 5006
    private_key_path: Optional[str] = None
    user: Optional[str] = None

    def __post_init__(self):
        if not self.lzy_mount:
            self.lzy_mount = os.getenv("LZY_MOUNT", default="/tmp/lzy")


class TerminalServer:
    jar_path = Path(os.path.dirname(__file__)) / ".." / "lzy-servant.jar"
    jar_path = jar_path.resolve().absolute()
    start_timeout_sec = 30

    def __init__(self, config: TerminalConfig, custom_log_file: str = "/tmp/lzy-log/custom_terminal_log",
                 terminal_log_path: str = "/tmp/lzy-log/terminal_log"):
        Path("/tmp/lzy-log").mkdir(parents=True, exist_ok=True)
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

        private_key_path = "null"
        if self._config.private_key_path is not None:
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
        if self._config.user is not None:
            env["USER"] = self._config.user

        terminal_args = [
            "--lzy-address", self._config.server_url,
            "--lzy-mount", self._config.lzy_mount,
            "--host", "localhost",
        ]
        if self._config.port is not None:
            terminal_args.extend((
                "--port", self._config.port,
            ))

        if self._config.private_key_path is not None:
            terminal_args.extend((
                "--private-key", private_key_path,
            ))
        terminal_args.append("terminal")

        # pylint: disable=consider-using-with
        self._pcs = subprocess.Popen(
            [
                "java",
                "-Dfile.encoding=UTF-8",
                "-Djava.util.concurrent.ForkJoinPool.common.parallelism=32",
                "-Djava.library.path=/usr/local/lib",
                f"-Dcustom.log.file={self._log_file}",
                f"-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=*:{self._config.debug_port}",
                "-jar", TerminalServer.jar_path,
                *terminal_args
            ],
            # stdout=self._terminal_log,
            # stderr=self._terminal_log,
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
        except OSError:  # TODO: find out what should be here
            return False
