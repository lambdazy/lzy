import pytest

import dataclasses

from lzy.types import VmSpec
from lzy.env.base import NotSpecified
from lzy.env.provisioning.provisioning import Provisioning, Any, NO_GPU
from lzy.env.provisioning.score import maximum_score_function, minimum_score_function
from lzy.exceptions import BadProvisioning


@pytest.fixture
def first():
    return Provisioning(cpu_count=1, gpu_count=Any)


@pytest.fixture
def second():
    return Provisioning(cpu_type='foo', gpu_type=Any, gpu_count=2)


def test_combine(first, second):
    assert first.combine(second) == Provisioning(
        cpu_count=1,
        cpu_type='foo',
        gpu_count=2,
        gpu_type=Any
    )

    assert second.combine(first) == Provisioning(
        cpu_count=1,
        cpu_type='foo',
        gpu_count=Any,
        gpu_type=Any,
    )


def test_validate(first, second):
    with pytest.raises(BadProvisioning):
        Provisioning(gpu_count=1).validate()

    with pytest.raises(BadProvisioning):
        Provisioning(gpu_type=NO_GPU, gpu_count=1).validate()

    with pytest.raises(BadProvisioning):
        Provisioning(gpu_type=NotSpecified, gpu_count=1).validate()

    first.combine(second).validate()
    second.combine(first).validate()


def test_filter():
    spec = VmSpec(
        cpu_type='cpu',
        gpu_type=NO_GPU,
        cpu_count=2,
        gpu_count=2,
        ram_size_gb=2
    )

    def get_provisioning(kwargs):
        yield Provisioning(**kwargs)

        for key, arg in kwargs.items():
            yield Provisioning(**{key: arg})

    good = dict(
        cpu_type='cpu',
        gpu_type=NO_GPU,
        cpu_count=1,
        gpu_count=1,
        ram_size_gb=1
    )
    for provisioning in get_provisioning(good):
        _prov_spec = provisioning._as_vm_spec()
        assert provisioning._filter_spec(_prov_spec, spec)

    equal = dict(
        cpu_type='cpu',
        gpu_type=NO_GPU,
        cpu_count=2,
        gpu_count=2,
        ram_size_gb=2,
    )
    for provisioning in get_provisioning(equal):
        _prov_spec = provisioning._as_vm_spec()
        assert provisioning._filter_spec(_prov_spec, spec)

    bad = dict(
        cpu_type='foo',
        gpu_type='bar',
        cpu_count=3,
        gpu_count=3,
        ram_size_gb=3,
    )
    for provisioning in get_provisioning(bad):
        _prov_spec = provisioning._as_vm_spec()
        assert not provisioning._filter_spec(_prov_spec, spec)

    any = dict(
        cpu_type=Any,
        gpu_type=Any,
        cpu_count=Any,
        gpu_count=Any,
        ram_size_gb=Any
    )

    for provisioning in get_provisioning(any):
        _prov_spec = provisioning._as_vm_spec()
        assert provisioning._filter_spec(_prov_spec, spec)

    not_specified = dict(
        cpu_type=NotSpecified,
        gpu_type=NotSpecified,
        cpu_count=NotSpecified,
        gpu_count=NotSpecified,
        ram_size_gb=NotSpecified
    )

    for provisioning in get_provisioning(not_specified):
        _prov_spec = provisioning._as_vm_spec()
        assert provisioning._filter_spec(_prov_spec, spec)


def test_filter_gpu():
    spec = VmSpec(
        cpu_type='cpu',
        gpu_type='gpu',
        cpu_count=2,
        gpu_count=2,
        ram_size_gb=2
    )

    for value in ('gpu', Any):
        provisioning = Provisioning(cpu_count=1, gpu_type=value)
        _prov_spec = provisioning._as_vm_spec()
        assert provisioning._filter_spec(_prov_spec, spec)

    for value in ('foo', NotSpecified, NO_GPU):
        provisioning = Provisioning(cpu_count=1, gpu_type=value)
        _prov_spec = provisioning._as_vm_spec()
        assert not provisioning._filter_spec(_prov_spec, spec)


def test_score_function(vm_specs, vm_spec_small, vm_spec_large):
    # NB: if we left gpu_type non-specified, it will coerce to default value NO_GPU
    # and will filter vm_spec_large from scoring:
    provisioning = Provisioning()
    with pytest.raises(BadProvisioning):
        provisioning.resolve_pool([vm_spec_large])

    provisioning = Provisioning(gpu_type=Any)
    assert provisioning.resolve_pool(vm_specs) == vm_spec_small

    provisioning = Provisioning(gpu_type=Any, score_function=maximum_score_function)
    assert provisioning.resolve_pool(vm_specs) == vm_spec_large

    provisioning = Provisioning(gpu_type=Any, score_function_default=maximum_score_function)
    assert provisioning.resolve_pool(vm_specs) == vm_spec_large

    provisioning = Provisioning(
        gpu_type=Any,
        score_function=minimum_score_function,
        score_function_default=maximum_score_function
    )
    assert provisioning.resolve_pool(vm_specs) == vm_spec_small
