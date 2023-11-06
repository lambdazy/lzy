from __future__ import annotations

import sys
import json
import pathlib

from typing import List, Tuple

import pytest
import google.protobuf.json_format

from ai.lzy.v1.workflow.workflow_pb2 import VmPoolSpec

import lzy.api.v1  # noqa
import lzy.config
from lzy.types import VmSpec
from tests.test_utils.workflow import TestLzyWorkflow


@pytest.fixture(autouse=True)
def skip_pypi_validation(monkeypatch):
    import envzy.pypi

    monkeypatch.setattr(envzy.pypi, 'VALIDATE_PYPI_INDEX_URL', False)


@pytest.fixture(autouse=True)
def test_lzy_workflow(monkeypatch):
    monkeypatch.setattr(lzy.api.v1.Lzy, '_workflow_class', TestLzyWorkflow)


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


@pytest.fixture(scope="session")
def vcr_config():
    return {"decode_compressed_response": True}


@pytest.fixture(scope='session')
def get_test_data_path():
    base = pathlib.Path(__file__).parent / 'test_data'

    def getter(*relative):
        return base.joinpath(*relative)

    return getter


@pytest.fixture(scope='function')
def with_test_modules(get_test_data_path, monkeypatch):
    with monkeypatch.context() as m:
        m.syspath_prepend(get_test_data_path())
        yield


# scope='class' because we use it via @pytest.mark.usefixtures('vm_proto_specs')
# on TestCase classes and somewhy it doesn't work with planned scope='session'
@pytest.fixture(scope='session')
def vm_proto_specs(request, get_test_data_path) -> Tuple[VmPoolSpec]:
    path = get_test_data_path('vm_proto_specs.json')

    with path.open('r') as f_:
        dict_data = json.load(f_)

    data = tuple(
        google.protobuf.json_format.ParseDict(_, VmPoolSpec())
        for _ in dict_data
    )

    return data


@pytest.fixture(scope='session')
def vm_specs(vm_proto_specs) -> Tuple[VmSpec]:
    return tuple(VmSpec.from_proto(p) for p in vm_proto_specs)


@pytest.fixture(scope='session')
def vm_proto_spec_large(vm_proto_specs) -> VmPoolSpec:
    for spec in vm_proto_specs:
        if spec.poolSpecName == 'large':
            return spec

    assert False, "unreachable"


@pytest.fixture(scope='session')
def vm_spec_large(vm_proto_spec_large) -> VmSpec:
    return VmSpec.from_proto(vm_proto_spec_large)


@pytest.fixture(scope='session')
def vm_proto_spec_small(vm_proto_specs) -> VmPoolSpec:
    for spec in vm_proto_specs:
        if spec.poolSpecName == 'small':
            return spec

    assert False, "unreachable"


@pytest.fixture(scope='session')
def vm_spec_small(vm_proto_spec_small) -> VmSpec:
    return VmSpec.from_proto(vm_proto_spec_small)


@pytest.fixture(scope='session')
def pypi_index_url():
    from envzy import PYPI_INDEX_URL_DEFAULT

    return PYPI_INDEX_URL_DEFAULT


@pytest.fixture(scope='session')
def pypi_index_url_testing():
    return 'https://test.pypi.org/simple'


@pytest.fixture(scope='session')
def env_prefix() -> pathlib.Path:
    return pathlib.Path(sys.exec_prefix)


@pytest.fixture(scope='session')
def site_packages(env_prefix: pathlib.Path) -> pathlib.Path:
    return env_prefix / "lib" / "python{}.{}".format(*sys.version_info) / "site-packages"


@pytest.fixture
def allowed_hosts() -> List[str]:
    return ['localhost', '127.0.0.1', '::1']
