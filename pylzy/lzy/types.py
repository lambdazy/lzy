from __future__ import annotations

import sys

from dataclasses import dataclass

from typing_extensions import Self, Protocol, final, runtime_checkable

from ai.lzy.v1.workflow.workflow_pb2 import VmPoolSpec as VmPoolSpecProto

# this code is analogue of `PlatformPath = type(Path())`
# but is more readable and it is understandable for mypy
if sys.platform == 'win32':
    from pathlib import WindowsPath as PlatformPath
else:
    from pathlib import PosixPath as PlatformPath


class File(PlatformPath):
    pass


@runtime_checkable
class VmResources(Protocol):
    cpu_type: str
    cpu_count: int
    gpu_type: str
    gpu_count: int
    ram_size_gb: int


VM_SPEC_PROTO_RELATION = {
    'cpu_type': 'cpuType',
    'cpu_count': 'cpuCount',
    'gpu_type': 'gpuType',
    'gpu_count': 'gpuCount',
    'ram_size_gb': 'ramGb'
}


@final
@dataclass
class VmSpec(VmResources):
    cpu_type: str
    cpu_count: int
    gpu_type: str
    gpu_count: int
    ram_size_gb: int

    @classmethod
    def from_proto(cls, spec: VmPoolSpecProto) -> Self:
        fields = {}

        for provisioning_field, spec_field in VM_SPEC_PROTO_RELATION.items():
            fields[provisioning_field] = getattr(spec, spec_field)

        return cls(**fields)
