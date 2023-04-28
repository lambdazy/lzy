import json
import pathlib
import pytest
import google.protobuf.json_format
from ai.lzy.v1.workflow.workflow_pb2 import VmPoolSpec


def set_unittest_fixture(request, value):
    assert value

    name = request.fixturename

    # if there no cls member or it is None,
    # it means that it is normal usage of fixture,
    # not with unittest
    if getattr(request, 'cls', None):
        setattr(request.cls, name, value)

    return value


@pytest.fixture(scope='session')
def get_test_data_path():
    base = pathlib.Path(__file__).parent / 'test_data'

    def getter(relative):
        return base / relative

    return getter


# scope='class' because we use it via @pytest.mark.usefixtures('vm_pool_specs')
# on TestCase classes and somewhy it doesn't work with planned scope='session'
@pytest.fixture(scope='class')
def vm_pool_specs(request, get_test_data_path):
    path = get_test_data_path('vm_pool_specs.json')

    with path.open('r') as f_:
        dict_data = json.load(f_)

    data = tuple(
        google.protobuf.json_format.ParseDict(_, VmPoolSpec())
        for _ in dict_data
    )

    return set_unittest_fixture(request, data)


@pytest.fixture(scope='class')
def vm_pool_spec_large(request, vm_pool_specs):
    result = None
    for spec in vm_pool_specs:
        if spec.poolSpecName == 'large':
            result = spec
            break

    return set_unittest_fixture(request, result)


@pytest.fixture(scope='class')
def vm_pool_spec_small(request, vm_pool_specs):
    result = None
    for spec in vm_pool_specs:
        if spec.poolSpecName == 'small':
            result = spec
            break

    return set_unittest_fixture(request, result)
