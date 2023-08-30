from __future__ import annotations

from dataclasses import dataclass, field
from typing import Sequence, TypeVar, Union, cast, TYPE_CHECKING

from typing_extensions import final

from lzy.env.base import NotSpecified, NotSpecifiedType
from lzy.env.provisioning.base import BaseProvisioning, VmSpecType_co
from lzy.env.provisioning.score import minimum_score_function, ScoreFunctionType
from lzy.exceptions import BadProvisioning
from lzy.logs.config import get_logger
from lzy.types import VmSpec

if TYPE_CHECKING:
    from lzy.env.mixin import WithEnvironmentType

NO_GPU = 'NO_GPU'
logger = get_logger(__name__)


@final
class AnyRequirement:
    def __repr__(self):
        return 'Any'

    def __eq__(self, other) -> bool:
        return type(self) is type(other)


class AnyStr(str):
    def __new__(cls):
        return super().__new__(cls, 'AnyStr')

    def __eq__(self, other) -> bool:
        return isinstance(other, str)

    def __ne__(self, other) -> bool:
        return not self.__eq__(other)


T = TypeVar('T')
Any = AnyRequirement()
WithAny = Union[AnyRequirement, T]

ProvisioningRequirement = Union[AnyRequirement, NotSpecifiedType, T]
IntegerRequirement = ProvisioningRequirement[int]
StringRequirement = ProvisioningRequirement[str]


def _coerce(value: ProvisioningRequirement[T], default: T) -> T:
    if value in (Any, NotSpecified):
        return cast(T, default)

    return cast(T, value)


@final
@dataclass
class Provisioning(BaseProvisioning):
    cpu_type: StringRequirement = NotSpecified
    cpu_count: IntegerRequirement = NotSpecified

    gpu_type: StringRequirement = NotSpecified
    gpu_count: IntegerRequirement = NotSpecified

    ram_size_gb: IntegerRequirement = NotSpecified

    score_function: Union[NotSpecifiedType, ScoreFunctionType] = field(
        default=NotSpecified,
        repr=False,
        compare=False,
    )
    score_function_default: ScoreFunctionType = field(
        default=minimum_score_function,
        repr=False,
        compare=False,
    )

    def __call__(self, subject: WithEnvironmentType) -> WithEnvironmentType:
        return subject.with_provisioning(self)

    def _as_vm_spec(self) -> VmSpec:
        # result will be used for filtering and weighting

        cpu_type: str
        gpu_type: str
        if self.cpu_type is Any:
            cpu_type = AnyStr()
        elif self.cpu_type is NotSpecified:
            cpu_type = AnyStr()
        else:
            cpu_type = self.cpu_type  # type: ignore[assignment]

        if self.gpu_type is Any:
            gpu_type = AnyStr()
        elif self.gpu_type is NotSpecified:
            gpu_type = NO_GPU
        else:
            gpu_type = self.gpu_type  # type: ignore[assignment]

        return VmSpec(
            cpu_type=cpu_type,
            gpu_type=gpu_type,
            cpu_count=_coerce(self.cpu_count, 0),
            gpu_count=_coerce(self.gpu_count, 0),
            ram_size_gb=_coerce(self.ram_size_gb, 0),
        )

    def _filter_spec(self, prov: VmSpec, spec: VmSpec) -> bool:
        if (
            spec.cpu_count < prov.cpu_count or
            spec.gpu_count < prov.gpu_count or
            spec.ram_size_gb < prov.ram_size_gb
        ):
            return False

        if (
            prov.cpu_type != spec.cpu_type or
            prov.gpu_type != spec.gpu_type
        ):
            return False

        return True

    def resolve_pool(self, vm_specs: Sequence[VmSpecType_co]) -> VmSpecType_co:
        self.validate()

        provisioning_spec = self._as_vm_spec()
        score_function = _coerce(self.score_function, self.score_function_default)

        if not vm_specs:
            raise BadProvisioning("there is no available pools on the server")

        spec_scores = []

        for vm_spec in vm_specs:
            if not self._filter_spec(provisioning_spec, vm_spec):
                continue

            score = score_function(provisioning_spec, vm_spec)
            spec_scores.append((score, vm_spec))
            logger.debug("eligible spec %r have score %f", vm_spec, score)

        if not spec_scores:
            vm_specs_repr = ','.join([repr(pool) for pool in vm_specs])
            raise BadProvisioning(
                f"not a single one available spec from {vm_specs_repr} eligible for requirements {self}"
            )

        spec_scores.sort(reverse=True)

        max_score, max_vm_spec = spec_scores[0]

        logger.info(
            "choose a spec %r with a max score %f for a provisioning requirements %r",
            max_vm_spec, max_score, self
        )

        return max_vm_spec

    def validate(self) -> None:
        if (
            _coerce(self.gpu_count, 0) > 0 and
            self.gpu_type in (NO_GPU, NotSpecified)
        ):
            raise BadProvisioning(f"gpu_type is set to {self.gpu_type} while gpu_count is {self.gpu_count}")
