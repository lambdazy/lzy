from functools import singledispatch
from typing import Union, overload

from ai.lzy.v1 import zygote_pb2
from lzy.env.env import AuxEnv, BaseEnv, EnvSpec


class env:
    @staticmethod
    @singledispatch
    def to(env_spec: EnvSpec) -> zygote_pb2.EnvSpec:
        return zygote_pb2.EnvSpec(
            auxEnv=env.to(env_spec.aux_env),
            baseEnv=env.to(env_spec.base_env),
        )

    @staticmethod
    @to.register
    def to(base_env: BaseEnv) -> zygote_pb2.BaseEnv:
        return zygote_pb2.BaseEnv(name=base_env.name)

    @staticmethod
    @to.register
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

    @staticmethod
    @singledispatch
    def from_(aux_env: zygote_pb2.AuxEnv) -> AuxEnv:
        pyenv = aux_env.pyenv
        local_modules = [mod.name for mod in pyenv.localModules]
        return AuxEnv(
            name=pyenv.name,
            conda_yaml=pyenv.yaml,
            local_modules_paths=local_modules,
        )

    @staticmethod
    @from_.register
    def from_(base_env: zygote_pb2.BaseEnv) -> BaseEnv:
        return BaseEnv(name=base_env.name)

    @staticmethod
    @from_.register
    def from_(env_spec: zygote_pb2.EnvSpec) -> EnvSpec:
        return EnvSpec(
            base_env=env.from_(env_spec.baseEnv),
            aux_env=env.from_(env_spec.auxEnv),
        )
