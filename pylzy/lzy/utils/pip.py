import ast
import logging
import os
import subprocess
import sys

from typing import Sequence, Optional, Dict, List
from packaging.version import Version
from lzy.utils.cached_property import cached_property


logger = logging.getLogger(__name__)


class Pip:
    # NB: pip before 10.0 doesn't have command pip config
    # Ubuntu 18.04 have pip==9.* by default for python3.7 and
    # a fresh ones already have normal pip
    min_version = Version("10.0")

    # and here we are making Pip class a singletone (borg, actually)
    # for more efficient work of cached properties
    _shared_state: dict = {}

    def __init__(self):
        self.__dict__ = self._shared_state

        if not self.version or self.version < self.min_version:
            raise RuntimeError(
                f'pip minimum required version is {self.min_version} '
                f'and you have {self.version}, please upgrade it'
            )

    def call(self, *args: str, cwd=None) -> str:
        command = [sys.executable, "-m", "pip"] + list(args)

        env = {**os.environ, **{"PIP_YES": "true", "PIP_DISABLE_PIP_VERSION_CHECK": "true"}}
        result = subprocess.check_output(
            command,
            cwd=cwd,
            env=env,
        )
        return result.decode()

    @cached_property
    def version(self) -> Optional[Version]:
        # NB: We doing try/except here in case of something wrong with
        # pip binary; in such case, there will be traceback at log
        # and we will raise of RuntimeError at Pip.__init__ about wrong version.
        # At other methods we are supposing that pip binary is fine and do
        # not capturing any exceptions.
        try:
            output = self.call('--version')
            if not output:
                return None

            # output should have a form:
            # pip <version> from <directory> (python <python version>)
            version_str = output.split()[1]
        except subprocess.CalledProcessError:
            logger.exception("Error while getting local pip version")
            return None

        return Version(version_str)

    @cached_property
    def config(self) -> Dict[str, str]:
        raw_config = self.call('config', 'list').strip()
        config: Dict[str, str] = {}

        for line in raw_config.splitlines():
            # line format example: global.index-url='https://pypi.yandex-team.ru/simple'
            line = line.strip()
            key, raw_value = line.split('=', 1)

            # NB: pip prints value via repr(), but it is too insecure to do eval
            # as invert function, so we are using "safe" literal eval (it can crash
            # with mailformed data, but thats it)
            # Also be aware, this method doesn't allow to properly read and parse
            # boolean values, look ConfigParser.getboolean method for details,
            # so we will read '1' and 'true' here as str values while pip treats it
            # as booleans.
            value = ast.literal_eval(raw_value)

            config[key] = value

        return config

    @cached_property
    def index_url(self) -> Optional[str]:
        index_url: Optional[str] = None

        # env var PIP_INDEX_URL is most valuable here;
        # at outer code you should value the more only explicit
        # value, as like when user pass --index-url to a pip CLI:
        # --index-url >> PIP_INDEX_URL >> value from config
        for prefix in (':env:', 'install', 'global'):
            index_url = self.config.get(f'{prefix}.index-url')
            if index_url:
                break

        # TODO: support additional_index_url config option
        if index_url:
            return index_url

        return None
