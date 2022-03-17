import os
from unittest import TestCase

from lzy.api.pkg_info import select_modules

from tests.test_modules.level1.level1 import Level1
from tests.test_modules.level1.level2_nb import level_foo


class ModulesSearchTests(TestCase):
    def test_modules_search(self):
        # Arrange
        level1 = Level1()

        # Act
        remote, local = select_modules({
            'level1': level1
        })

        # Assert
        self.assertEqual("echo", level1.echo())
        cwd = os.getcwd()

        self.assertTrue(cwd in local)
        self.assertTrue(cwd + "/test_modules" in local)
        self.assertTrue(cwd + "/test_modules/level1" in local)
        self.assertTrue(cwd + "/test_modules/level1/level2" in local)
        self.assertTrue(cwd + "/test_modules/level1/level2/level3" in local)
        self.assertTrue(cwd + "/test_modules/level1/level2/level3/level3.py" in local)
        self.assertTrue(cwd + "/test_modules/level1/level2/level2.py" in local)
        self.assertTrue(cwd + "/test_modules/level1/level1.py" in local)
        self.assertEqual({"PyYAML", "boto3"}, set(remote.keys()))

    def test_modules_search_2(self):
        _, local = select_modules({
            'level_foo': level_foo
        })
        cwd = os.getcwd()
        self.assertTrue(cwd in local)
        self.assertTrue(cwd + "/test_modules" in local)
        self.assertTrue(cwd + "/test_modules/level1" in local)
        self.assertTrue(cwd + "/test_modules/level1/level2" in local)
        self.assertTrue(cwd + "/test_modules/level1/level2/level3" in local)
        self.assertTrue(cwd + "/test_modules/level1/level2/level3/level3.py" in local)
        self.assertTrue(cwd + "/test_modules/level1/level2/level2.py" in local)
        self.assertTrue(cwd + "/test_modules/level1/level2_nb" in local)
