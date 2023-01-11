from dataclasses import dataclass
from enum import Enum
from typing import Optional


class CpuType(Enum):
    ICE_LAKE = "standard-v3"
    CASCADE_LAKE = "standard-v2"
    BROADWELL = "standard-v1"


class GpuType(Enum):
    NO_GPU = "<none>"
    V100 = "gpu-standard-v2"
    A100 = "gpu-standard-v3"


@dataclass(frozen=True)
class Provisioning:
    cpu_type: Optional[str] = None
    cpu_count: Optional[int] = None

    gpu_type: Optional[str] = None
    gpu_count: Optional[int] = None

    ram_size_gb: Optional[int] = None

    @staticmethod
    def default() -> "Provisioning":
        return Provisioning(
            cpu_type=CpuType.CASCADE_LAKE.value,
            cpu_count=2,
            gpu_type=GpuType.NO_GPU.value,
            gpu_count=0,
            ram_size_gb=2,
        )

    def override(self, other: Optional["Provisioning"] = None) -> "Provisioning":
        if other is None:
            other = self.default()

        return Provisioning(
            cpu_type=self.cpu_type if self.cpu_type else other.cpu_type,
            cpu_count=self.cpu_count if self.cpu_count is not None else other.cpu_count,
            gpu_type=self.gpu_type if self.gpu_type else other.gpu_type,
            gpu_count=self.gpu_count if self.gpu_count is not None else other.gpu_count,
            ram_size_gb=self.ram_size_gb
            if self.ram_size_gb is not None
            else other.ram_size_gb,
        )
