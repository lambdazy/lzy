import re
import pathlib
import logging


MYPY_WHITELIST = [
    r'lzy/'
]

DOCTEST_BLACKLIST = [
    r'google/'
]


def pytest_collection_modifyitems(config, items):
    # NB: pytest-mypy ignores 'files' and 'exclude' config
    # options from mypy.ini because of the reasons.
    # So here we are filtering mypy items by ourselves.
    mypy = config.pluginmanager.getplugin('mypy')

    pylzy = pathlib.Path(__file__).parent

    for item in items[:]:
        if not isinstance(item, mypy.MypyItem):
            continue

        path = pathlib.Path(item.fspath).relative_to(pylzy)
        path = str(path)

        if not all(
            re.match(pattern, path) for pattern in MYPY_WHITELIST
        ):
            items.remove(item)
            continue

        if any(
            re.match(pattern, path) for pattern in DOCTEST_BLACKLIST
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
