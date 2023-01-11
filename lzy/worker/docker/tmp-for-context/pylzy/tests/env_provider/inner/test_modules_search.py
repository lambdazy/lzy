import os
from unittest import TestCase

from lzy.py_env.api import PyEnvProvider
from lzy.py_env.py_env_provider import AutomaticPyEnvProvider

from ..local_file import bar


class ModulesSearchTests(TestCase):
    def test_modules_search_relative(self):
        # Arrange
        provider: PyEnvProvider = AutomaticPyEnvProvider()
        bar_func = bar

        # Act
        env = provider.provide({"bar_func": bar_func})
        local = env.local_modules_path

        # Assert
        os.chdir(os.path.dirname(__file__))
        directory = os.path.dirname(os.getcwd())

        self.assertFalse(directory + "/inner" in local)
        self.assertTrue(directory in local)
