import pytest

from pypi_simple import PYPI_SIMPLE_ENDPOINT

import lzy.exceptions
import lzy.utils.pypi

TEST_PYPI = 'https://test.pypi.org/simple'


@pytest.mark.vcr
def test_valid_pypi_index_url():
    lzy.utils.pypi.validate_pypi_index_url(PYPI_SIMPLE_ENDPOINT)
    lzy.utils.pypi.validate_pypi_index_url(TEST_PYPI)


@pytest.mark.vcr
def test_non_pypi_index_url():
    with pytest.raises(lzy.exceptions.BadPypiIndex):
        lzy.utils.pypi.validate_pypi_index_url('https://example.com')


@pytest.mark.block_network
def test_pypi_index_url_without_a_scheme():
    # here will be no net interactions
    with pytest.raises(lzy.exceptions.BadPypiIndex):
        lzy.utils.pypi.validate_pypi_index_url('test.pypi.org/simple')


@pytest.mark.skip(reason="vcr can't record ConnectionError and we don't wanna to do DNS requests")
@pytest.mark.vcr
def test_nonexisting_pypi_index_url():
    with pytest.raises(lzy.exceptions.BadPypiIndex):
        lzy.utils.pypi.validate_pypi_index_url('https://foo.bar.example.com')


@pytest.mark.vcr
def test_check_package_version_exists():
    assert lzy.utils.pypi.check_package_version_exists(
        pypi_index_url=TEST_PYPI,
        name='pip',
        version='10.0.0',
    )

    assert not lzy.utils.pypi.check_package_version_exists(
        pypi_index_url=TEST_PYPI,
        name='pip',
        version='9999.9999.9999',
    )

    assert not lzy.utils.pypi.check_package_version_exists(
        pypi_index_url=TEST_PYPI,
        name='my-awesome-non-existing-package',
        version='10.0.0',
    )
