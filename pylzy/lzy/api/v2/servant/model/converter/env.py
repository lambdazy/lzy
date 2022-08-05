from functools import singledispatch
from typing import Optional, Union, cast, overload

from ai.lzy.v1 import zygote_pb2
from lzy.env.env import AuxEnv, BaseEnv, EnvSpec

ENV = Union[
    EnvSpec,
    BaseEnv,
    AuxEnv,
]
grpcENV = Union[
    zygote_pb2.EnvSpec,
    zygote_pb2.BaseEnv,
    zygote_pb2.AuxEnv,
]


@singledispatch
def _to(env_spec: EnvSpec) -> zygote_pb2.EnvSpec:
    return zygote_pb2.EnvSpec(
        auxEnv=_to(env_spec.aux_env),
        baseEnv=_to(env_spec.base_env),
    )


@_to.register  # type: ignore[no-redef]
def _to(base_env: BaseEnv) -> zygote_pb2.BaseEnv:
    return zygote_pb2.BaseEnv(name=base_env.name)


@_to.register  # type: ignore[no-redef]
def _to(aux_env: AuxEnv) -> zygote_pb2.AuxEnv:
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


@overload
def to(obj: EnvSpec) -> zygote_pb2.EnvSpec:
    ...


@overload
def to(obj: AuxEnv) -> zygote_pb2.AuxEnv:
    ...


@overload
def to(obj: BaseEnv) -> zygote_pb2.BaseEnv:
    ...


def to(obj: ENV) -> grpcENV:
    return cast(grpcENV, _to(obj))


@singledispatch
def _fr(aux_env: zygote_pb2.AuxEnv) -> AuxEnv:
    pyenv = aux_env.pyenv
    local_modules = [mod.name for mod in pyenv.localModules]
    return AuxEnv(
        name=pyenv.name,
        conda_yaml=pyenv.yaml,
        local_modules_paths=local_modules,
    )


@_fr.register  # type: ignore[no-redef]
def _fr(base_env: zygote_pb2.BaseEnv) -> BaseEnv:
    return BaseEnv(name=base_env.name)


@_fr.register  # type: ignore[no-redef]
def _fr(env_spec: zygote_pb2.EnvSpec) -> EnvSpec:
    return EnvSpec(
        base_env=from_(env_spec.baseEnv),
        aux_env=from_(env_spec.auxEnv),
    )


@overload
def from_(obj: zygote_pb2.EnvSpec) -> EnvSpec:
    ...


@overload
def from_(obj: zygote_pb2.AuxEnv) -> AuxEnv:
    ...


@overload
def from_(obj: zygote_pb2.BaseEnv) -> BaseEnv:
    ...


def from_(obj: grpcENV) -> ENV:
    return cast(ENV, _fr(obj))
