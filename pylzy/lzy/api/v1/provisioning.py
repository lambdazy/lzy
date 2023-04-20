import dataclasses

from enum import Enum
from typing import (
    Any as typingAny,
    Callable,
    ClassVar,
    Dict,
    List,
    Optional,
    Sequence,
    Tuple,
    TypeVar,
    Union,
    cast,
)

from ai.lzy.v1.workflow.workflow_pb2 import VmPoolSpec
from lzy.exceptions import BadProvisioning
from lzy.logs.config import get_logger
from lzy.utils.format import pretty_protobuf

__all__ = [
    'CpuType',
    'GpuType',
    'Provisioning',
    'minimum_score_function',
    'maximum_score_function',
    'Any',
    'StringRequirement',
    'IntegerRequirement',
]


logger = get_logger(__name__)


DEFAULT_WEIGHTS = {
    'gpu': 100.0,
    'cpu': 20.0,
    'ram': 1.0
}


class AnyRequirement:
    def __repr__(self):
        return 'Any'


Any = AnyRequirement()

IntegerRequirement = Union[int, AnyRequirement]
StringRequirement = Union[str, AnyRequirement]
TRequirement = TypeVar('TRequirement')


def _coerce(value: Optional[TRequirement], default: Optional[TRequirement]) -> Optional[TRequirement]:
    if (
        value is Any or
        value is typingAny or
        value is None
    ):
        return default

    return cast(TRequirement, value)


class CpuType(Enum):
    ICE_LAKE = "Intel Ice Lake"
    CASCADE_LAKE = "Intel Cascade Lake"
    BROADWELL = "Intel Broadwell"


class GpuType(Enum):
    NO_GPU = "NO_GPU"
    V100 = "V100"
    A100 = "A100"
    T4 = "T4"


def maximum_score_function(requested: "Provisioning", spec: "Provisioning") -> float:
    """Score function that scores big machines the max.
    Use it when you need the best spec available.
    """

    d_gpu = spec.gpu_count - requested.gpu_count
    d_cpu = spec.cpu_count - requested.cpu_count
    d_ram = spec.ram_size_gb- requested.ram_size_gb

    return d_gpu * DEFAULT_WEIGHTS['gpu'] + d_cpu * DEFAULT_WEIGHTS['cpu'] + d_ram * DEFAULT_WEIGHTS['ram']


def minimum_score_function(requested: "Provisioning", spec: "Provisioning") -> float:
    """Score function that scores small machines the max.
    Use it when you need smallest spec available that matches you requirements.

    """

    max_score = maximum_score_function(requested=requested, spec=spec)
    return 1.0 / max_score if max_score else float("inf")


@dataclasses.dataclass(frozen=True)
class Provisioning:
    cpu_type: Optional[StringRequirement] = None
    cpu_count: Optional[IntegerRequirement] = None

    gpu_type: Optional[StringRequirement] = None
    gpu_count: Optional[IntegerRequirement] = None

    ram_size_gb: Optional[IntegerRequirement] = None

    score_function: Callable[["Provisioning", "Provisioning"], float] = dataclasses.field(
        default=minimum_score_function,
        repr=False,
        compare=False,
    )

    proto_relation: ClassVar[Dict[str, str]] = {
        'cpu_type': 'cpuType',
        'cpu_count': 'cpuCount',
        'gpu_type': 'gpuType',
        'gpu_count': 'gpuCount',
        'ram_size_gb': 'ramGb'
    }

    @classmethod
    def from_proto(cls, spec: VmPoolSpec) -> "Provisioning":
        fields = {}

        for provisioning_field, spec_field in cls.proto_relation.items():
            fields[provisioning_field] = getattr(spec, spec_field)

        return cls(**fields)

    def _filter_spec(self, spec: "Provisioning") -> bool:
        if (
            spec.cpu_count < self.cpu_count or
            spec.gpu_count < self.gpu_count or
            spec.ram_size_gb < self.ram_size_gb
        ):
            return False

        if (
            self.cpu_type and self.cpu_type != spec.cpu_type or
            self.gpu_type and self.gpu_type != spec.gpu_type
        ):
            return False

        return True

    def resolve_pool(self, pool_specs: Sequence[VmPoolSpec]) -> VmPoolSpec:
        # For a purposes of filtering and scoring we need finalize out Provisioning:
        # replace Any and None with something eligeble for comparsion.
        # At this moment Any and None have no difference, this difference
        # only matters for merging provisionings from workflow an ops.
        finalized = self.override(
            cpu_count=_coerce(self.cpu_count, 0),
            gpu_count=_coerce(self.gpu_count, 0),
            ram_size_gb=_coerce(self.ram_size_gb, 0),
            cpu_type=_coerce(self.cpu_type, None),
            gpu_type=_coerce(self.gpu_type, None),
        )
        finalized._validate()

        spec_scores = []

        for proto_spec in pool_specs:
            provisioning = self.from_proto(proto_spec)

            if finalized._filter_spec(provisioning):
                continue

            score = self.score_function(finalized, provisioning)
            spec_scores.append((score, proto_spec, provisioning))
            logger.debug("eligible spec %r have score %f", provisioning, score)

        if not spec_scores:
            pool_specs_repr = [pretty_protobuf(pool) for pool in pool_specs]
            raise BadProvisioning(f"not a single one available spec from {pool_specs_repr!r} eligible for requirements {self}")

        spec_scores.sort(reverse=True)

        max_score, max_proto_spec, max_provisioning = spec_scores[0]

        logger.info("choose a spec %r with a max score %f for a provisioning requirements %r", max_provisioning, max_score, self)

        return max_proto_spec

    def override(
        self,
        other: Optional["Provisioning"] = None,
        **kwargs,
    ) -> "Provisioning":
        if other and kwargs:
            raise TypeError('usage of args and kwargs at the same time is forbidden')

        if not other and not kwargs:
            raise TypeError('missing provisioning arguments')

        new_kwargs = {}

        if other:
            kwargs = dataclasses.asdict(other)

        for field in dataclasses.fields(self):
            name = field.name

            value = kwargs.pop(name, None)
            # If rvalue is None we must save lvalue
            # because it practically means user didn't specify new value and we
            # saving old one.
            # But if rvalue 0 or Any, we will use new, specified value.
            if value is None:
                value = getattr(self, name)

            new_kwargs[name] = value

        if kwargs:
            raise TypeError(
                "got an unexpected keyword arguments '{}'"
                .format(', '.join(kwargs))
            )

        return self.__class__(**new_kwargs)

    def _validate(self) -> None:
        if self.gpu_count > 0 and self.gpu_type == GpuType.NO_GPU.value:
            raise BadProvisioning(f"gpu_type is set to {self.gpu_type} while gpu_count is {self.gpu_count}")
