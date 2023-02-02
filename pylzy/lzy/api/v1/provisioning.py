from dataclasses import dataclass
from enum import Enum
from typing import Optional


class CpuType(Enum):
    ICE_LAKE = "Intel Ice Lake"
    CASCADE_LAKE = "Intel Cascade Lake"
    BROADWELL = "Intel Broadwell"


class GpuType(Enum):
    NO_GPU = "NO_GPU"
    V100 = "V100"
    A100 = "A100"
    T4 = "T4"


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

    def override(self, other: "Provisioning") -> "Provisioning":
        return Provisioning(
            cpu_type=other.cpu_type if other.cpu_type else self.cpu_type,
            cpu_count=other.cpu_count if other.cpu_count else self.cpu_count,
            gpu_type=other.gpu_type if other.gpu_type else self.gpu_type,
            gpu_count=other.gpu_count if other.gpu_count else self.gpu_count,
            ram_size_gb=other.ram_size_gb if other.ram_size_gb else self.ram_size_gb
        )

    def validate(self) -> None:
        if self.cpu_type is None:
            raise ValueError("cpu_type is not set")
        if self.cpu_count is None:
            raise ValueError("cpu_count is not set")
        if self.ram_size_gb is None:
            raise ValueError("ram_size_gb is not set")
        if self.gpu_type is None:
            raise ValueError("gpu_type is not set")
        if self.gpu_count is None:
            raise ValueError("gpu_count is not set")

        if self.gpu_count > 0 and self.gpu_type == GpuType.NO_GPU.value:
            raise ValueError(f"gpu_type is set to {self.gpu_type} while gpu_count is {self.gpu_count}")
