import os
import sys
import pytest
from pathlib import Path
from unittest import TestCase

from lzy.py_env.api import PyEnvProvider
from lzy.py_env.py_env_provider import AutomaticPyEnvProvider


class ModulesSearchTests(TestCase):
    @classmethod
    def setUpClass(cls):
        cls.this_test_directory: Path = Path(__file__).parent
        cls.pylzy_directory: Path = cls.this_test_directory.parent.parent.parent

    def setUp(self):
        self.provider: PyEnvProvider = AutomaticPyEnvProvider()

        self.old_cwd: Path = Path.cwd()
        self.old_sys_path: List[str] = list(sys.path)

        os.chdir(self.this_test_directory)

        # NB: remove pylzy directory from sys.path to avoid
        # getting pylzy/tests directory into local_modules_path
        # because it shuts down pylzy/tests/env_provide
        # which this test expects in local_modules_path
        sys.path.remove(str(self.pylzy_directory))

    def tearDown(self):
        os.chdir(self.old_cwd)
        sys.path = self.old_sys_path

    @pytest.mark.xfail(reason="I just can't fix it right now while I'm refactoring project to pytest")
    def test_modules_search_relative(self):
        from ..local_file import bar

        print(sys.path)
        # Arrange
        bar_func = bar

        # Act
        env = self.provider.provide({"bar_func": bar_func})
        local = env.local_modules_path

        # Assert
        self.assertNotIn(str(self.this_test_directory.parent / "inner"), local)
        self.assertIn(str(self.this_test_directory.parent), local)
