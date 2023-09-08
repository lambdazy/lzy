from __future__ import annotations

import inspect
import collections

import pytest

from lzy.api.v1 import provisioning, Provisioning
from lzy.api.v1 import ManualPythonEnv, manual_python_env
from lzy.api.v1 import AutoPythonEnv, auto_python_env
from lzy.api.v1 import DockerContainer, docker_container
from lzy.api.v1 import NoContainer, no_container
from lzy.api.v1 import env_vars
from lzy.env.mixin import WithEnvironmentMixin
from lzy.utils.inspect import get_annotations


TC = collections.namedtuple('TC', ('func', 'klass', 'method', 'extra_klass_params'))


def get_env_method(name, dispatch=False):
    method = getattr(WithEnvironmentMixin, name)
    if dispatch:
        method = method.register.__self__.kwargs_func
    return method


TEST_CASES = (
   TC(provisioning,
      Provisioning,
      get_env_method('with_provisioning', True),
      ['score_function_default', 'score_function']),
   TC(manual_python_env,
      ManualPythonEnv,
      get_env_method('with_manual_python_env'),
      []),
   TC(auto_python_env,
      AutoPythonEnv,
      get_env_method('with_auto_python_env'),
      ['env_explorer_factory', '_env_explorer']),
   TC(docker_container,
      DockerContainer,
      get_env_method('with_docker_container'),
      []),
   TC(no_container,
      NoContainer,
      get_env_method('with_no_container'),
      []),
   TC(env_vars,
      None,
      get_env_method('with_env_vars', True),
      []),
)


@pytest.fixture(params=TEST_CASES, ids=[tc.func.__name__ for tc in TEST_CASES])
def test_case(request):
    yield request.param


def test_kw_only_params(test_case):
    def assert_kw_only_params(func):
        for param in inspect.signature(func).parameters.values():
            if param.name == 'self':
                continue

            assert param.kind in (param.KEYWORD_ONLY, param.VAR_KEYWORD), \
                'all parameters must be kwonly'

    assert_kw_only_params(test_case.func)
    assert_kw_only_params(test_case.method)
    # TODO: uncomment after dropping 3.9
    # assert_kw_only_params(test_case.klass)


def test_similar_names(test_case):
    func_names = set(inspect.signature(test_case.func).parameters)
    method_names = set(inspect.signature(test_case.method).parameters) - {'self'}

    assert func_names == method_names

    if test_case.klass:
        klass_names = set(inspect.signature(test_case.klass).parameters) - {'self'}
        klass_names -= set(test_case.extra_klass_params)

        assert klass_names == func_names


def test_types(test_case):
    def get_types(func):
        result = get_annotations(func, eval_str=True)
        for param in ['self', 'return'] + test_case.extra_klass_params:
            result.pop(param, None)

        return result

    func_types = get_types(test_case.func)
    method_types = get_types(test_case.method)

    assert func_types == method_types

    if test_case.klass:
        klass_types = get_types(test_case.klass)

        assert func_types == klass_types
