__package__ = None

import os
from unittest import TestCase

from lzy.py_env.api import PyEnvProvider
from lzy.py_env.py_env_provider import AutomaticPyEnvProvider

from modules_for_tests.level1.level1 import Level1
from modules_for_tests.level1.level2_nb import level_foo


class ModulesSearchTests(TestCase):
    def setUp(self):
        self.provider: PyEnvProvider = AutomaticPyEnvProvider()

    def test_modules_search(self):
        # Arrange
        level1 = Level1()
        os.chdir(os.path.dirname(__file__))
        directory = os.path.abspath(os.path.join(os.getcwd(), os.pardir))

        # Act
        env = self.provider.provide({"level1": level1})
        remote = env.libraries
        local = env.local_modules_path

        # Assert
        self.assertEqual("echo", level1.echo())
        # noinspection DuplicatedCode
        self.assertTrue(directory + "/modules_for_tests" in local)
        self.assertFalse(directory + "/modules_for_tests/level1" in local)
        self.assertFalse(directory + "/modules_for_tests/level1/level2" in local)
        self.assertFalse(directory + "/modules_for_tests/level1/level2/level3" in local)
        self.assertFalse(directory + "/modules_for_tests/level1/level2/level3/level3.py" in local)
        self.assertFalse(directory + "/modules_for_tests/level1/level2/level2.py" in local)
        self.assertFalse(directory + "/modules_for_tests/level1/level1.py" in local)
        self.assertEqual({"PyYAML", "cloudpickle"}, set(remote.keys()))

    def test_modules_search_2(self):
        env = self.provider.provide({"level_foo": level_foo})
        local = env.local_modules_path
        os.chdir(os.path.dirname(__file__))
        directory = os.path.abspath(os.path.join(os.getcwd(), os.pardir))

        # noinspection DuplicatedCode
        self.assertTrue(directory + "/modules_for_tests" in local)
        self.assertFalse(directory + "/modules_for_tests/level1" in local)
        self.assertFalse(directory + "/modules_for_tests/level1/level2" in local)
        self.assertFalse(directory + "/modules_for_tests/level1/level2/level3" in local)
        self.assertFalse(directory + "/modules_for_tests/level1/level2/level3/level3.py" in local)
        self.assertFalse(directory + "/modules_for_tests/level1/level2/level2.py" in local)
        self.assertFalse(directory + "/modules_for_tests/level1/level2_nb" in local)
