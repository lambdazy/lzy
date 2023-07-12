import json
import pathlib
import pytest
import google.protobuf.json_format
from ai.lzy.v1.workflow.workflow_pb2 import VmPoolSpec
from lzy.types import VmSpec


@pytest.fixture(scope="module")
def vcr_cassette_dir(request, get_test_data_path) -> str:
    module = request.node.fspath  # current test file

    # /home/<user>/.../lzy/pylzy/tests/utils/test_gprc.py
    module_filename = pathlib.Path(module)

    # /home/<user>/.../lzy/pylzy/tests/
    conftest_dir = pathlib.Path(__file__).parent

    # utils/test_grpc.py
    rel_module_path = module_filename.relative_to(conftest_dir)

    # utils/test_grpc
    cassete_dir = rel_module_path.with_suffix('')

    # /home/<user>/.../lzy/pylzy/tests/test_data/cassetes/utils/test_grpc/
    return str(get_test_data_path("cassettes", cassete_dir))


@pytest.fixture(scope="module")
def vcr_config():
    return {"decode_compressed_response": True}


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

    def getter(*relative):
        return base.joinpath(*relative)

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
def vm_specs(vm_pool_specs):
    return tuple(VmSpec.from_proto(p) for p in vm_pool_specs)


@pytest.fixture(scope='class')
def vm_pool_spec_large(request, vm_pool_specs):
    result = None
    for spec in vm_pool_specs:
        if spec.poolSpecName == 'large':
            result = spec
            break

    return set_unittest_fixture(request, result)


@pytest.fixture(scope='class')
def vm_spec_large(request, vm_pool_specs):
    result = None
    for spec in vm_pool_specs:
        if spec.poolSpecName == 'large':
            return VmSpec.from_proto(spec)

    assert False, "unreachable"


@pytest.fixture(scope='class')
def vm_pool_spec_small(request, vm_pool_specs):
    result = None
    for spec in vm_pool_specs:
        if spec.poolSpecName == 'small':
            result = spec
            break

    return set_unittest_fixture(request, result)


@pytest.fixture(scope='class')
def vm_spec_small(request, vm_pool_specs):
    result = None
    for spec in vm_pool_specs:
        if spec.poolSpecName == 'small':
            return VmSpec.from_proto(spec)

    assert False, "unreachable"


@pytest.fixture(scope='session')
def pypi_index_url():
    from pypi_simple import PYPI_SIMPLE_ENDPOINT

    return PYPI_SIMPLE_ENDPOINT


@pytest.fixture(scope='session')
def pypi_index_url_testing():
    return 'https://test.pypi.org/simple'
