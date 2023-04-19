__package__ = None

import os
import sys
from pathlib import Path
from unittest import TestCase

from lzy.py_env.api import PyEnvProvider
from lzy.py_env.py_env_provider import AutomaticPyEnvProvider


class ModulesSearchTests(TestCase):
    @classmethod
    def setUpClass(cls):
        cls.this_test_directory: Path = Path(__file__).parent
        cls.test_data_dir = cls.this_test_directory.parent / 'test_data'

    def setUp(self):
        self.provider: PyEnvProvider = AutomaticPyEnvProvider()

        self.old_cwd: Path = Path.cwd()
        self.old_sys_path: List[str] = list(sys.path)

        os.chdir(self.this_test_directory)
        sys.path.append(str(self.test_data_dir))

    def tearDown(self):
        os.chdir(self.old_cwd)
        sys.path = self.old_sys_path

    def test_modules_search(self):
        # Arrange
        from modules_for_tests.level1.level1 import Level1
        from modules_for_tests_2.level import Level

        level1 = Level1()
        level = Level()

        # Act
        env = self.provider.provide({"level": level, "level1": level1})
        remote = env.libraries
        local_modules_path = env.local_modules_path

        # Assert
        self.assertEqual("echo", level1.echo())
        self.assertEqual(
            3 if sys.version_info < (3, 10) else 2,  # typing extensions is a standard module starting from 3.10
            len(local_modules_path)
        )

        # noinspection DuplicatedCode
        for path in (
            "modules_for_tests",
            "modules_for_tests_2",
        ):
            full_path = self.test_data_dir / path
            self.assertIn(str(full_path), local_modules_path)

        for path in (
            "modules_for_tests/level1",
            "modules_for_tests/level1/level2",
            "modules_for_tests/level1/level2/level3",
            "modules_for_tests/level1/level2/level3/level3.py",
            "modules_for_tests/level1/level2/level2.py",
            "modules_for_tests/level1/level1.py",
        ):
            full_path = self.test_data_dir / path
            self.assertNotIn(str(full_path), local_modules_path)

        self.assertEqual({"PyYAML", "cloudpickle"}, set(remote.keys()))

    def test_modules_search_2(self):
        from modules_for_tests.level1.level2_nb import level_foo

        env = self.provider.provide({"level_foo": level_foo})
        local_modules_path = env.local_modules_path

        # noinspection DuplicatedCode
        self.assertEqual(
            2 if sys.version_info < (3, 10) else 1,  # typing extensions is a standard module starting from 3.10
            len(local_modules_path)
        )

        for path in (
            "modules_for_tests",
        ):
            full_path = self.test_data_dir / path
            self.assertIn(str(full_path), local_modules_path)

        for path in (
            "modules_for_tests/level1",
            "modules_for_tests/level1/level2",
            "modules_for_tests/level1/level2/level3",
            "modules_for_tests/level1/level2/level3/level3.py",
            "modules_for_tests/level1/level2/level2.py",
            "modules_for_tests/level1/level2_nb",
        ):
            full_path = self.test_data_dir / path
            self.assertNotIn(str(full_path), local_modules_path)

    def test_exceptional_builtin(self):
        import _functools

        env = self.provider.provide({"_functools": _functools})

        self.assertEqual(len(env.local_modules_path), 0)
        self.assertEqual(len(env.libraries), 0)
