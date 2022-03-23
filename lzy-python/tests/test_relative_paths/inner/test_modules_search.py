import os
from unittest import TestCase

from lzy.api.pkg_info import select_modules

from ..local_file import bar


class ModulesSearchTests(TestCase):
    def test_modules_search_relative(self):
        # Arrange
        bar_func = bar

        # Act
        remote, local = select_modules({
            'bar_func': bar_func
        })

        # Assert
        os.chdir(os.path.dirname(__file__))
        directory = os.path.dirname(os.getcwd())

        self.assertFalse(directory + "/inner" in local)
        self.assertTrue(directory in local)
