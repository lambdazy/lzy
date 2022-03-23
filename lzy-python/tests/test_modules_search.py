__package__ = None

import os
from unittest import TestCase

from lzy.api.pkg_info import select_modules

from test_modules.level1.level1 import Level1
from test_modules.level1.level2_nb import level_foo
from some_imported_file import bar


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
        os.chdir(os.path.dirname(__file__))
        directory = os.path.abspath(os.path.join(os.getcwd(), os.pardir))

        self.assertTrue(directory + "/test_modules" in local)
        self.assertFalse(directory + "/test_modules/level1" in local)
        self.assertFalse(directory + "/test_modules/level1/level2" in local)
        self.assertFalse(directory + "/test_modules/level1/level2/level3" in local)
        self.assertFalse(directory + "/test_modules/level1/level2/level3/level3.py" in local)
        self.assertFalse(directory + "/test_modules/level1/level2/level2.py" in local)
        self.assertFalse(directory + "/test_modules/level1/level1.py" in local)
        self.assertEqual({"PyYAML", "boto3"}, set(remote.keys()))

    def test_modules_search_2(self):
        _, local = select_modules({
            'level_foo': level_foo
        })
        os.chdir(os.path.dirname(__file__))
        directory = os.path.abspath(os.path.join(os.getcwd(), os.pardir))
        self.assertTrue(directory + "/test_modules" in local)
        self.assertFalse(directory + "/test_modules/level1" in local)
        self.assertFalse(directory + "/test_modules/level1/level2" in local)
        self.assertFalse(directory + "/test_modules/level1/level2/level3" in local)
        self.assertFalse(directory + "/test_modules/level1/level2/level3/level3.py" in local)
        self.assertFalse(directory + "/test_modules/level1/level2/level2.py" in local)
        self.assertFalse(directory + "/test_modules/level1/level2_nb" in local)

    def test_modules_search_3(self):
        _, local = select_modules({
            'bar': bar
        })
        os.chdir(os.path.dirname(__file__))
        cwd = os.getcwd()
        self.assertTrue(cwd + '/some_imported_file.py')
