import re
import pathlib
import fnmatch
import logging


MYPY_WHITELIST = [
    r'lzy/',
    r'tests/env/',
    r'tests/core/'
    r'tests/conftest.py',
]

DOCTEST_BLACKLIST = [
    r'tests/test_data/'
]

FLAKES_BLACKLIST = [
    r'ai/',
    r'tests/scenarios/',
]


def try_fix_flakes_globs():
    try:
        import pytest_flakes
    except ImportError:
        return

    def __call__(self, path):
        result = set()
        for (glob, ignlist) in self.ignores:
            if not glob or fnmatch.fnmatch(path, glob):
                if ignlist is None:
                    return None
                result.update(set(ignlist))
        return sorted(result)

    pytest_flakes.Ignorer.__call__ = __call__


try_fix_flakes_globs()


def pytest_collection_modifyitems(config, items):
    # NB: pytest-mypy ignores 'files' and 'exclude' config
    # options from mypy.ini because of the reasons.
    # So here we are filtering mypy items by ourselves.
    mypy = config.pluginmanager.getplugin('mypy')
    flakes = config.pluginmanager.getplugin('flakes')

    pylzy = pathlib.Path(__file__).parent

    for item in items[:]:
        path = pathlib.Path(item.fspath).relative_to(pylzy)
        path = str(path)

        if any(
            re.match(pattern, path) for pattern in DOCTEST_BLACKLIST
        ):
            items.remove(item)
            continue

        if mypy and isinstance(item, mypy.MypyItem) and not any(
            re.match(pattern, path) for pattern in MYPY_WHITELIST
        ):
            items.remove(item)
            continue

        if flakes and isinstance(item, flakes.FlakesItem) and any(
            re.match(pattern, path) for pattern in FLAKES_BLACKLIST
        ):
            items.remove(item)
            continue


def pytest_configure():
    # NB: filter debug messages from filelock library which is used
    # inside pytest-mypy and prints a lot of meaningless lines
    logging.getLogger('filelock').setLevel(logging.INFO)


def pytest_ignore_collect(path, config):
    pylzy = pathlib.Path(__file__).parent
    rel_path = pathlib.Path(path).relative_to(pylzy)
    rel_path = str(rel_path)

    # XXX: doctest collecting (sic!) will fail until protobuf 4.0
    # because of old protobuf have an bug, which doesn't allow to
    # import two identical protobufs
    if rel_path == 'google/protobuf/any_pb2.py':
        return True

    # it causes problems, because doctests during collection
    # modifies sys.path and adding lzy_test_project/src into sys.path.
    if rel_path.startswith('tests/test_data/'):
        return True
