import dataclasses
from pathlib import Path

import pytest

from lzy.env.explorer.classify import ModuleClassifier
from lzy.env.explorer.packages import LocalPackage, PypiDistribution, LocalDistribution


@pytest.fixture(scope='function')
def classifier(pypi_index_url) -> ModuleClassifier:
    return ModuleClassifier(pypi_index_url=pypi_index_url)


@pytest.mark.block_network
def test_classify_local_packages(
    with_test_modules,
    get_test_data_path,
    monkeypatch,
    classifier: ModuleClassifier,
) -> None:
    monkeypatch.syspath_prepend(get_test_data_path('namespace'))

    import modules_for_tests.level1.level1 as level1
    import modules_for_tests.foo as foo
    import empty_module

    # regular module
    assert classifier.classify([level1]) == frozenset([LocalPackage(
        name='modules_for_tests',
        paths=frozenset([str(get_test_data_path('modules_for_tests'))]),
        is_binary=False
    )])

    # two modules within one namespace but different locations
    assert classifier.classify([level1, foo]) == frozenset([LocalPackage(
        name='modules_for_tests',
        paths=frozenset([
            str(get_test_data_path('modules_for_tests')),
            str(get_test_data_path('namespace', 'modules_for_tests')),
        ]),
        is_binary=False
    )])

    # toplevel module without a package
    assert classifier.classify([empty_module]) == frozenset([LocalPackage(
        name='empty_module',
        paths=frozenset([str(get_test_data_path('empty_module.py'))]),
        is_binary=False
    )])


@pytest.mark.vcr
def test_classify_pypi_packages(classifier: ModuleClassifier, pypi_index_url: str) -> None:
    assert classifier.classify([pytest]) == frozenset({
        PypiDistribution(
            name='pytest',
            version='.'.join(str(v) for v in pytest.version_tuple),
            pypi_index_url=pypi_index_url,
        )
    })


@pytest.mark.vcr
def test_classify_local_distribution(classifier: ModuleClassifier, virtualenv_path: Path) -> None:
    # NB: lzy_test_project located at test_data/lzy_test_project and gets installed by tox while
    # tox venv preparing
    import lzy_test_project
    import lzy_test_project.foo

    etalon = LocalDistribution(
        name='lzy-test-project',
        paths=frozenset({
            f'{virtualenv_path}/lib/python3.7/site-packages/lzy_test_project',
            f'{virtualenv_path}/lib/python3.7/site-packages/lzy_test_project-3.0.0.dist-info'
        }),
        is_binary=False,
        version='3.0.0',
        bad_paths=frozenset({f'{virtualenv_path}/bin/lzy_test_project_bin'})
    )

    assert classifier.classify([lzy_test_project]) == frozenset({etalon})
    assert classifier.classify([lzy_test_project.foo]) == frozenset({dataclasses.replace(etalon, is_binary=True)})


@pytest.mark.block_network
def test_classify_editable_distribution(classifier: ModuleClassifier, get_test_data_path) -> None:
    # NB: lzy_test_project_editable located at test_data/lzy_test_project_editable and gets installed by tox while
    # tox venv preparing
    import lzy_test_project_editable

    assert classifier.classify([lzy_test_project_editable]) == frozenset({
        LocalPackage(
            name='lzy_test_project_editable',
            paths=frozenset({
                f'{get_test_data_path()}/lzy_test_project_editable/src/lzy_test_project_editable'
            }),
            is_binary=False
        )
    })
