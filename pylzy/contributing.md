### About the tests

* **You have to run scripts/build.sh before invoking any tests**

* `tox` - run all tests for available Python on your computer

* `tox -e py37` - run only py37 tox environment

* `tox -- <opts>` will pass `<opts>` to the underlying pytest run

* `tox -- -vv` or `pytest -vv` - run tests in verbose mode

* `tox -- tests/env_provider/test_modules_search.py` or `pytest tests/env_provider/test_modules_search.py` - run tests only for one test file

* `tox -- -m mypy` or `pytest --mypy -m mypy` - run only mypy tests

* You can combine all these options: `tox -- -e py39 -m mypy -vv lzy/api/v1/remote/workflow_service_client.py`

* Same thing with `pycodestyle`, `doctest`, and `cov`, you can run what you need

* `# type: ignore` will shut down mypy error on this string

* mypy and doctest file filtering is managed by `conftest.py`, not by `tox.ini`/`mypy.ini`

* `tests/scenarios` - are executables for java integrational tests, so it is skipping part of tox/pytest checks; however, they are imported by doctest and this is the reason why all executables must contain `if __name__ == '__main__'`
