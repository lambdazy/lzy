# pylint: disable=import-outside-toplevel

from __future__ import annotations as _annotations

from lzy.api.v1.runtime import Runtime
from lzy.api.v1.local.runtime import LocalRuntime
from lzy.api.v1.remote.runtime import RemoteRuntime

from lzy.core.op import op
from lzy.core.lzy import Lzy, lzy_auth

from lzy.env.environment import LzyEnvironment
from lzy.env.container.docker import DockerContainer, DockerPullPolicy
from lzy.env.container.no_container import NoContainer
from lzy.env.provisioning import score
from lzy.env.provisioning.provisioning import Provisioning, Any as AnyProvisioning
from lzy.env.python.auto import AutoPythonEnv
from lzy.env.python.manual import ManualPythonEnv
from lzy.env.shortcuts import (
    docker_container,
    no_container,
    provisioning,
    auto_python_env,
    manual_python_env,
    env_vars,
)

from lzy.logs.config import configure_logging as _configure_logging


_configure_logging()


def whiteboard(name: str):
    from lzy.api.v1.whiteboards import whiteboard_

    def wrap(cls):
        return whiteboard_(cls, name)

    return wrap
