import os
from unittest import TestCase

from lzy.api.pkg_info import select_modules

from .test_modules_2 import Level1
from test_modules_2 import bar


class ModulesSearchTests(TestCase):
    def test_modules_search_relative(self):
        # Arrange
        level1 = Level1()

        # Act
        remote, local = select_modules({
            'level1': level1
        })

        # Assert
        cwd = os.getcwd()

        self.assertTrue(cwd in local)
        self.assertFalse(cwd + "/test_modules_2" in local)

    def test_modules_search_absolute(self):
        # Arrange
        bar_func = bar

        # Act
        remote, local = select_modules({
            'bar_func': bar_func
        })

        # Assert
        cwd = os.getcwd()

        self.assertTrue(cwd + "/test_modules_2" in local)
        self.assertFalse(cwd in local)
