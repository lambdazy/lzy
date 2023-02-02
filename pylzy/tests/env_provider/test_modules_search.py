__package__ = None

import os
from unittest import TestCase

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
        local = env.local_modules_path

        # Assert
        self.assertEqual("echo", level1.echo())
        self.assertEqual(2, len(local))
        # noinspection DuplicatedCode
        self.assertTrue(directory + "/modules_for_tests" in local)
        self.assertTrue(directory + "/modules_for_tests_2" in local)
        self.assertFalse(directory + "/modules_for_tests/level1" in local)
        self.assertFalse(directory + "/modules_for_tests/level1/level2" in local)
        self.assertFalse(directory + "/modules_for_tests/level1/level2/level3" in local)
        self.assertFalse(directory + "/modules_for_tests/level1/level2/level3/level3.py" in local)
        self.assertFalse(directory + "/modules_for_tests/level1/level2/level2.py" in local)
        self.assertFalse(directory + "/modules_for_tests/level1/level1.py" in local)
        self.assertEqual({"PyYAML", "cloudpickle"}, set(remote.keys()))

    def test_modules_search_2(self):
        from modules_for_tests.level1.level2_nb import level_foo
        env = self.provider.provide({"level_foo": level_foo})
        local = env.local_modules_path
        os.chdir(os.path.dirname(__file__))
        directory = os.path.abspath(os.path.join(os.getcwd(), os.pardir))

        # noinspection DuplicatedCode
        self.assertEqual(1, len(local))
        self.assertTrue(directory + "/modules_for_tests" in local)
        self.assertFalse(directory + "/modules_for_tests/level1" in local)
        self.assertFalse(directory + "/modules_for_tests/level1/level2" in local)
        self.assertFalse(directory + "/modules_for_tests/level1/level2/level3" in local)
        self.assertFalse(directory + "/modules_for_tests/level1/level2/level3/level3.py" in local)
        self.assertFalse(directory + "/modules_for_tests/level1/level2/level2.py" in local)
        self.assertFalse(directory + "/modules_for_tests/level1/level2_nb" in local)

    def test_exceptional_builtin(self):
        import _functools
        env = self.provider.provide({"_functools": _functools})
        self.assertTrue(len(env.local_modules_path) == 0)
        self.assertTrue(len(env.libraries) == 0)
