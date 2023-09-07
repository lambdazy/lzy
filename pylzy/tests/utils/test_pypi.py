import pytest

import lzy.config
import lzy.exceptions
import lzy.utils.pypi


@pytest.fixture(autouse=True)
def skip_pypi_validation(monkeypatch):
    monkeypatch.setattr(lzy.config, 'skip_pypi_validation', False)


@pytest.mark.vcr
def test_valid_pypi_index_url(pypi_index_url, pypi_index_url_testing):
    assert not lzy.config.skip_pypi_validation

    lzy.utils.pypi.validate_pypi_index_url(pypi_index_url)
    lzy.utils.pypi.validate_pypi_index_url(pypi_index_url_testing)


@pytest.mark.vcr
def test_non_pypi_index_url(monkeypatch):
    assert not lzy.config.skip_pypi_validation

    with pytest.raises(lzy.exceptions.BadPypiIndex):
        lzy.utils.pypi.validate_pypi_index_url('https://example.com')

    monkeypatch.setattr(lzy.config, 'skip_pypi_validation', True)
    lzy.utils.pypi.validate_pypi_index_url('https://example.com')


def test_pypi_index_url_without_a_scheme(monkeypatch, pypi_index_url_testing):
    assert not lzy.config.skip_pypi_validation

    bad_pypi_index_url = pypi_index_url_testing.split('//')[1]

    # here will be no net interactions
    with pytest.raises(lzy.exceptions.BadPypiIndex):
        lzy.utils.pypi.validate_pypi_index_url(bad_pypi_index_url)

    monkeypatch.setattr(lzy.config, 'skip_pypi_validation', True)
    lzy.utils.pypi.validate_pypi_index_url(bad_pypi_index_url)


@pytest.mark.skip(reason="vcr can't record ConnectionError and we don't wanna to do DNS requests")
@pytest.mark.vcr
def test_nonexisting_pypi_index_url():
    assert not lzy.config.skip_pypi_validation

    with pytest.raises(lzy.exceptions.BadPypiIndex):
        lzy.utils.pypi.validate_pypi_index_url('https://foo.bar.example.com')


@pytest.mark.vcr
def test_check_package_version_exists(pypi_index_url_testing):
    assert lzy.utils.pypi.check_package_version_exists(
        pypi_index_url=pypi_index_url_testing,
        name='pip',
        version='10.0.0',
    )

    assert not lzy.utils.pypi.check_package_version_exists(
        pypi_index_url=pypi_index_url_testing,
        name='pip',
        version='9999.9999.9999',
    )

    assert not lzy.utils.pypi.check_package_version_exists(
        pypi_index_url=pypi_index_url_testing,
        name='my-awesome-non-existing-package',
        version='10.0.0',
    )
