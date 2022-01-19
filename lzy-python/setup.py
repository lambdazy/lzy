from abc import ABC
from datetime import datetime
from typing import List

import setuptools
from setuptools import Distribution, Command
from setuptools.command.install import install
from setuptools.command.sdist import sdist
from wheel.bdist_wheel import bdist_wheel  # type: ignore


class RunMixin(Command, ABC):
    def initialize_options(self):
        self.dev = None
        super().initialize_options()

    def run(self):
        self.set_distr_version_and_name(self.distribution, self.dev is not None)
        super().run()

    def set_distr_version_and_name(self, distribution: Distribution,
                                   is_dev: bool):
        distribution.metadata.version = read_version()
        if is_dev:
            suffix = f".dev{datetime.today().strftime('%Y%m%d')}"
            distribution.metadata.version += suffix  # type: ignore
            distribution.metadata.name = 'pylzy-nightly'


class _bdist_wheel(RunMixin, bdist_wheel):
    user_options = bdist_wheel.user_options + [
        ('dev', None, "Build nightly package")
    ]


class _sdist(RunMixin, sdist):
    user_options = sdist.user_options + [
        ('dev', None, "Build nightly package")
    ]


class _install(RunMixin, install):
    user_options = install.user_options + [
        ('dev', None, "Build nightly package")
    ]


def read_version(path='version'):
    with open(path) as file:
        return file.read().rstrip()


def read_requirements() -> List[str]:
    requirements = []
    with open('requirements.txt', 'r') as file:
        for line in file:
            requirements.append(line.rstrip())
    return requirements


setuptools.setup(
    name='pylzy',
    version=read_version(),
    author='ʎzy developers',
    include_package_data=True,
    package_data={
        'lzy': ['lzy/lzy-servant.jar']
    },
    install_requires=read_requirements(),
    packages=['lzy', 'lzy/api', 'lzy/api/whiteboard', 'lzy/api/_proxy',
              'lzy/model', 'lzy/servant', 'lzy/api/pkg_info'],
    python_requires='>=3.7',
    cmdclass={
        'install': _install,
        'bdist_wheel': _bdist_wheel,
        'sdist': _sdist,
    }
)
