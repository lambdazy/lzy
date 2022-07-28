from functools import singledispatch
from typing import Union, overload

from ai.lzy.v1 import zygote_pb2
from lzy.env.env import AuxEnv, BaseEnv, EnvSpec

POSSIBLE_INP_TYPES = Union[
    AuxEnv,
    BaseEnv,
    EnvSpec,
]

POSSIBLE_OUT_TYPES = Union[
    zygote_pb2.AuxEnv,
    zygote_pb2.BaseEnv,
    zygote_pb2.EnvSpec,
]


@singledispatch
def _to(obj: POSSIBLE_INP_TYPES) -> POSSIBLE_OUT_TYPES:
    raise TypeError(type(obj))


@overload
@_to.register
def to(env_spec: EnvSpec) -> zygote_pb2.EnvSpec:
    return zygote_pb2.EnvSpec(
        auxEnv=to(env_spec.aux_env),
        baseEnv=to(env_spec.base_env),
    )


@overload
@_to.register
def to(base_env: BaseEnv) -> zygote_pb2.BaseEnv:
    return zygote_pb2.BaseEnv(name=base_env.name)


@overload
@_to.register
def to(aux_env: AuxEnv) -> zygote_pb2.AuxEnv:
    local_modules = [
        zygote_pb2.LocalModule(
            name=name,
            # uri="",  # will be set later
        )
        for name in aux_env.local_modules_paths
    ]
    return zygote_pb2.AuxEnv(
        pyenv=zygote_pb2.PythonEnv(
            name=aux_env.name,
            yaml=aux_env.conda_yaml,
            localModules=local_modules,
        )
    )


def to(obj: POSSIBLE_INP_TYPES) -> POSSIBLE_OUT_TYPES:  # type: ignore[misc]
    return _to(obj)
