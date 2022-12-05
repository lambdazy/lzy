from abc import ABC
from datetime import datetime
from typing import List

import setuptools
from setuptools import Command, Distribution
from setuptools.command.install import install
from setuptools.command.install_egg_info import install_egg_info
from setuptools.command.sdist import sdist
from wheel.bdist_wheel import bdist_wheel  # type: ignore


class RunMixin(Command, ABC):
    def initialize_options(self):
        self.dev = None
        super().initialize_options()

    def run(self):
        self.set_distr_version_and_name(self.distribution, self.dev is not None)
        super().run()

    def set_distr_version_and_name(self, distribution: Distribution, is_dev: bool):
        distribution.metadata.version = read_version()
        if is_dev:
            today = datetime.today().strftime("%Y%m%d")
            distribution.metadata.version += f".dev{today}"  # type: ignore
            distribution.metadata.name = "pylzy-nightly"


class _install_egg_info(RunMixin, install_egg_info):
    user_options = bdist_wheel.user_options + [("dev", None, "Build nightly package")]


class _bdist_wheel(RunMixin, bdist_wheel):
    user_options = bdist_wheel.user_options + [("dev", None, "Build nightly package")]


class _sdist(RunMixin, sdist):
    user_options = sdist.user_options + [("dev", None, "Build nightly package")]


class _install(RunMixin, install):
    user_options = install.user_options + [("dev", None, "Build nightly package")]


def read_version(path="version/version"):
    with open(path) as file:
        return file.read().rstrip()


def read_requirements() -> List[str]:
    requirements = []
    with open("requirements.txt", "r") as file:
        for line in file:
            requirements.append(line.rstrip())
    return requirements


setuptools.setup(
    name="pylzy",
    version=read_version(),
    author="ʎzy developers",
    include_package_data=True,
    package_data={
        "lzy": [
            "lzy/lzy-servant.jar",
        ],
        "ai/lzy/v1": [
            "**/*.pyi",
            "*.pyi",
        ],
        "version": ["version"],
    },
    install_requires=read_requirements(),
    packages=[
        "lzy",
        "lzy/api",
        "lzy/api/v1",
        "lzy/api/v1/whiteboard",
        "lzy/api/v1/servant/model",
        "lzy/api/v1/servant",
        "lzy/api/v1/pkg_info",
        "lzy/api/v2",
        "lzy/api/v2/local",
        "lzy/api/v2/utils",
        "lzy/api/v2/remote",
        "lzy/api/v2/remote/model",
        "lzy/api/v2/remote/model/converter",
        "lzy/proxy",
        "lzy/py_env",
        "lzy/storage",
        "lzy/storage/async_",
        "lzy/storage/deprecated",
        "lzy/cli",
        "lzy/serialization",
        "lzy/injections",
        "lzy/whiteboards",
        "lzy/utils",
        "ai/",
        "ai/lzy",
        "ai/lzy/v1",
        "ai/lzy/v1/common",
        "ai/lzy/v1/validation",
        "ai/lzy/v1/whiteboard",
        "ai/lzy/v1/workflow",
        "version",
    ],
    python_requires=">=3.7",
    cmdclass={
        "install": _install,
        "bdist_wheel": _bdist_wheel,
        "sdist": _sdist,
        "install_egg_info": _install_egg_info,
    },
    entry_points={
        "console_scripts": ["lzy-terminal=lzy.cli.terminal_runner:console_main"],
    },
)
