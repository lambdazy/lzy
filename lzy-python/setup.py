import os
import platform
import re
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import setuptools
import subprocess

from abc import ABC, abstractmethod
from distutils.command.install import install
from typing import Optional, List

JAVA_VERSION_MIN = 11


def is_mac() -> bool:
    return platform.system() == "Darwin"


class StageResult:
    def __init__(self, result: bool, error_message: str = None):
        self._error_message = error_message
        self._result = result

    def done(self) -> bool:
        return self._result

    def error_message(self) -> Optional[str]:
        return f"\n\n########################################\n\n" \
               f"FAILED TO INSTALL LZY\n\nREASON: {self._error_message}\n\n" \
               f"########################################"


class Stage(ABC):
    @abstractmethod
    def check(self) -> bool:
        pass

    @abstractmethod
    def apply(self) -> StageResult:
        pass

    def run(self) -> StageResult:
        if self.check():
            return StageResult(True)
        return self.apply()


class JavaCheckStage(Stage):
    _version_pattern = re.compile(r'"(\d+.\d+).*"')

    def __init__(self):
        super().__init__()
        self._error = None
        self._checked = False

    def check(self) -> bool:
        try:
            out = str(subprocess.check_output(['java', '-version'],
                                              stderr=subprocess.STDOUT))
            
            search = self._version_pattern.search(out)
            if not search:
                self._error = "Wrong java version"
                return False
            
            version = int(float(search.groups()[0]))
            if version < JAVA_VERSION_MIN:
                self._error = "Java >= 11 is required"
                return False
            return True
        except (subprocess.CalledProcessError, FileNotFoundError):
            self._error = "Java is required (>= 11)"
            return False

    def apply(self) -> StageResult:
        if self._checked:
            return StageResult(True)
        # Do we really need to try to install Java here?
        return StageResult(False, self._error)


class FuseCheckStage(Stage):
    def __init__(self):
        super().__init__()
        self._error = None

    def check(self) -> bool:
        if is_mac():
            if os.path.isfile("/usr/local/lib/libosxfuse.dylib"):
                return True
            else:
                self._error = "macFUSE lib is required https://osxfuse.github.io"
                return False
        # it is better to pass this stage if we are not sure
        return True

    def apply(self) -> StageResult:
        return StageResult(False, self._error)


class Installer(install):
    def run(self):
        stages: List[Stage] = [JavaCheckStage(), FuseCheckStage()]
        for s in stages:
            result = s.run()
            if not result.done():
                raise ValueError(result.error_message())
        super().run()


def read_version(path='version'):
    with open(path) as file:
        return file.read().rstrip()


def read_requirements() -> List[str]:
    requirements = []
    with open('requirements.txt', 'r') as file:
        for line in file:
            requirements.append(line.rstrip())
    return requirements


@dataclass
class PackageConf:
    name: str

    @property
    def version(self):
        return read_version()


class DevPackageConf(PackageConf):
    @property
    def version(self):
        return read_version() + '.dev' + datetime.today().strftime('%Y%m%d')


conf_map = {
    'refs/heads/main': PackageConf(name='pylzy'),
    'refs/heads/dev': DevPackageConf(name='pylzy-nightly')
}


def _get_git_ref():
    with open('HEAD') as git_head:
        return git_head.readline().split()[-1]


try:
    print(_get_git_ref())
    conf = conf_map[_get_git_ref()]
except:
    raise ValueError("Trying to install from other branches than master or dev")


setuptools.setup(
    name=conf.name,
    version=conf.version,
    author='ʎzy developers',
    include_package_data=True,
    package_data={
        'lzy': ['lzy/lzy-servant.jar']
    },
    packages=['lzy', 'lzy/api', 'lzy/api/whiteboard', 'lzy/api/_proxy',
              'lzy/model', 'lzy/servant', 'lzy/api/pkg_info'],
    requirements=read_requirements(),
    python_requires='>=3.7',
    cmdclass={
        'installer.py': Installer  # type: ignore
    }
)
