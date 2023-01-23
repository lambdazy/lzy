import os

from mock import patch
from typing import Dict, List
from unittest import TestCase

import yaml

from lzy.api.v1 import Lzy, op
from lzy.api.v1.dvc import dvc_file_name, params_file_name, requirements_file_name
from lzy.types import File
from mocks import RuntimeMock, StorageRegistryMock


# noinspection PyUnusedLocal
@op
def f1(size: int, config: File) -> int:
    return size ** 2


# noinspection PyUnusedLocal
@op
def f2(name: str, cap: int) -> str:
    return 'ok'


# noinspection PyUnusedLocal
@op
def f3(data: File, ratio: float) -> bool:
    return True


# noinspection PyUnusedLocal
@op
def f4(size: int, config: File = File('cfg.yaml')) -> int:
    return size ** 2


# noinspection PyUnusedLocal
@op
def f5(name: str, cap: int, ready: bool = True) -> str:
    return 'ok'


class Foo:
    ...


# noinspection PyUnusedLocal
@op
def f6(foo: Foo) -> None:
    ...


@patch('sys.argv', ['main.py'])
class DvcTests(TestCase):
    def setUp(self):
        self.lzy = Lzy(runtime=RuntimeMock(), storage_registry=StorageRegistryMock())
        self.yaml_loader = yaml.Loader

    def tearDown(self):
        for fn in (requirements_file_name, dvc_file_name, params_file_name):
            if os.path.exists(fn):
                os.remove(fn)

    def assert_dvc_files(self, dvc_yaml_expected: Dict, params_yaml_expected: Dict, deps_expected: List[str]):
        with open(dvc_file_name) as f:
            dvc_yaml_actual = yaml.load(f.read(), self.yaml_loader)

        with open(params_file_name) as f:
            params_yaml_actual = yaml.load(f.read(), self.yaml_loader)

        with open(requirements_file_name) as f:
            deps_actual = f.read().split('\n')

        self.assertEqual(dvc_yaml_expected, dvc_yaml_actual)
        self.assertEqual(params_yaml_expected, params_yaml_actual)
        self.assertEqual(deps_expected, deps_actual)

    def test_drop_intermediate_args(self):
        # var 'cap' will be dropped
        with self.lzy.workflow('test', dvc=True):
            cap = f1(7, File('cfg.yaml'))
            _ = f2('foo', cap)

        self.assert_dvc_files(
            {
                'stages': {
                    'main': {
                        'cmd': 'python main.py',
                        'deps': [
                            'dvc_requirements.txt',
                            'ai',
                            'test_dvc.py',
                            'mocks.py',
                            'cfg.yaml',
                        ],
                        'params': ['size', 'name']
                    }
                }
            },
            {'name': 'foo', 'size': 7},
            [
                'mock==5.0.1',
                'PyYAML==6.0',
                'pylzy==0.0.50',
                'googleapis-common-protos==1.57.0',
                'grpcio==1.50.0',
                'serialzy==0.0.12',
            ]
        )

    # TODO: clarify what to do with defaults
    def test_kwargs_and_default_values(self):
        with self.lzy.workflow('test', dvc=True):
            _ = f4(42)
            _ = f5('bar', cap=7, ready=False)

        self.assert_dvc_files(
            {
                'stages': {
                    'main': {
                        'cmd': 'python main.py',
                        'deps': [
                            'dvc_requirements.txt',
                            'ai',
                            'test_dvc.py',
                            'mocks.py',
                            # 'cfg.yaml',
                        ],
                        'params': ['size', 'name', 'cap', 'ready']
                    }
                }
            },
            {'cap': 7, 'name': 'bar', 'ready': False, 'size': 42},
            [
                'mock==5.0.1',
                'PyYAML==6.0',
                'pylzy==0.0.50',
                'googleapis-common-protos==1.57.0',
                'grpcio==1.50.0',
                'serialzy==0.0.12',
            ]
        )

    def test_var_used_in_multiple_ops(self):
        num = 15
        cfg = File('cfg.yaml')
        with self.lzy.workflow('test', dvc=True):
            _ = f1(num, cfg)
            _ = f2('foo', num)
            _ = f4(num, cfg)

        self.assert_dvc_files(
            {
                'stages': {
                    'main': {
                        'cmd': 'python main.py',
                        'deps': [
                            'dvc_requirements.txt',
                            'ai',
                            'test_dvc.py',
                            'mocks.py',
                            'cfg.yaml',
                        ],
                        'params': ['size', 'name']  # 'name' taken from f1, first @op using this var
                    }
                }
            },
            {'size': 15, 'name': 'foo'},
            [
                'mock==5.0.1',
                'PyYAML==6.0',
                'pylzy==0.0.50',
                'googleapis-common-protos==1.57.0',
                'grpcio==1.50.0',
                'serialzy==0.0.12',
            ]
        )

    def test_arg_name_collisions(self):
        ...

    def test_arg_is_not_file_or_primitive(self):
        with self.assertRaisesRegex(ValueError, 'you can use only File or primitive types in @op\'s using DVC'):
            with self.lzy.workflow('test', dvc=True):
                f6(Foo())
