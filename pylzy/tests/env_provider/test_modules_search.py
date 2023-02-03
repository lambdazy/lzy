__package__ = None

import os
import sys
from pathlib import Path
from unittest import TestCase

from lzy.api.v1.utils.files import sys_path_parent
from lzy.py_env.api import PyEnvProvider
from lzy.py_env.py_env_provider import AutomaticPyEnvProvider


class ModulesSearchTests(TestCase):
    def setUp(self):
        self.provider: PyEnvProvider = AutomaticPyEnvProvider()

    def test_modules_search(self):
        # Arrange
        from modules_for_tests.level1.level1 import Level1
        from modules_for_tests_2.level import Level
        level1 = Level1()
        level = Level()
        os.chdir(os.path.dirname(__file__))
        directory = os.path.abspath(os.path.join(os.getcwd(), os.pardir))

        # Act
        env = self.provider.provide({"level": level, "level1": level1})
        remote = env.libraries
        local_modules_path = env.local_modules_path

        # Assert
        self.assertEqual("echo", level1.echo())
        self.assertEqual(3 if sys.version_info < (3, 10) else 2,
                         len(local_modules_path))  # typing extensions is a standard module starting from 3.10

        for local in local_modules_path:
            prefix = sys_path_parent(local)
            path = (Path(local).relative_to(prefix))
            self.assertIsNotNone(path)

        # noinspection DuplicatedCode
        self.assertTrue(directory + "/modules_for_tests" in local_modules_path)
        self.assertTrue(directory + "/modules_for_tests_2" in local_modules_path)
        self.assertTrue(directory + "/modules_for_tests_2" in local_modules_path)
        self.assertFalse(directory + "/modules_for_tests/level1" in local_modules_path)
        self.assertFalse(directory + "/modules_for_tests/level1/level2" in local_modules_path)
        self.assertFalse(directory + "/modules_for_tests/level1/level2/level3" in local_modules_path)
        self.assertFalse(directory + "/modules_for_tests/level1/level2/level3/level3.py" in local_modules_path)
        self.assertFalse(directory + "/modules_for_tests/level1/level2/level2.py" in local_modules_path)
        self.assertFalse(directory + "/modules_for_tests/level1/level1.py" in local_modules_path)
        self.assertEqual({"PyYAML", "cloudpickle"}, set(remote.keys()))

    def test_modules_search_2(self):
        from modules_for_tests.level1.level2_nb import level_foo
        env = self.provider.provide({"level_foo": level_foo})
        local_modules_path = env.local_modules_path
        os.chdir(os.path.dirname(__file__))
        directory = os.path.abspath(os.path.join(os.getcwd(), os.pardir))

        # noinspection DuplicatedCode
        self.assertEqual(2 if sys.version_info < (3, 10) else 1,
                         len(local_modules_path))  # typing extensions is a standard module starting from 3.10
        for local in local_modules_path:
            prefix = sys_path_parent(local)
            path = (Path(local).relative_to(prefix))
            self.assertIsNotNone(path)
        self.assertTrue(directory + "/modules_for_tests" in local_modules_path)
        self.assertFalse(directory + "/modules_for_tests/level1" in local_modules_path)
        self.assertFalse(directory + "/modules_for_tests/level1/level2" in local_modules_path)
        self.assertFalse(directory + "/modules_for_tests/level1/level2/level3" in local_modules_path)
        self.assertFalse(directory + "/modules_for_tests/level1/level2/level3/level3.py" in local_modules_path)
        self.assertFalse(directory + "/modules_for_tests/level1/level2/level2.py" in local_modules_path)
        self.assertFalse(directory + "/modules_for_tests/level1/level2_nb" in local_modules_path)

    def test_exceptional_builtin(self):
        import _functools
        env = self.provider.provide({"_functools": _functools})
        self.assertTrue(len(env.local_modules_path) == 0)
        self.assertTrue(len(env.libraries) == 0)
