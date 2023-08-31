import pytest

from lzy.api.v1 import Lzy

from tests.api.v1.mocks import RuntimeMock, StorageRegistryMock


@pytest.fixture
def lzy():
    return Lzy(runtime=RuntimeMock(), storage_registry=StorageRegistryMock())
