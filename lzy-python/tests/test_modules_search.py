from unittest import TestCase

from lzy.api.pkg_info import select_modules

from tests.test_modules.level1.level1 import Level1
from tests.test_modules.level1.level2_nb import level_foo
import base


class ModulesSearchTests(TestCase):
    def test_modules_search(self):
        # Arrange
        level1 = Level1()

        # Act
        remote, parents, local = select_modules({
            'level1': level1
        })

        # Assert
        self.assertEqual("echo", level1.echo())
        module_names = []
        for module in local:
            module_names.append(module.__name__)
        parent_names = []
        for module in parents:
            parent_names.append(module.__name__)

        self.assertEqual([
            'tests',
            'tests.test_modules',
            'tests.test_modules.level1',
            'tests.test_modules.level1.level2',
            'tests.test_modules.level1.level2.level3'], parent_names)
        self.assertEqual([
            'tests.test_modules.level1.level2.level3.level3',
            'tests.test_modules.level1.level2.level2',
            'tests.test_modules.level1.level1'
        ], module_names)
        self.assertEqual({"PyYAML", "s3fs"}, set(remote.keys()))

    def test_modules_search_2(self):
        _, parents, local = select_modules({
            'level_foo': level_foo
        })
        module_names = []
        for module in local:
            module_names.append(module.__name__)
        parent_names = []
        for module in parents:
            parent_names.append(module.__name__)
        self.assertEqual([
            'tests',
            'tests.test_modules',
            'tests.test_modules.level1',
            'tests.test_modules.level1.level2',
            'tests.test_modules.level1.level2.level3'
        ], parent_names)
        self.assertEqual([
            'tests.test_modules.level1.level2.level3.level3',
            'tests.test_modules.level1.level2.level2',
            'tests.test_modules.level1.level2_nb'
        ], module_names)

    def test_modules_search_without_parents(self):
        _, parents, local = select_modules({
            'base': base
        })
        module_names = []
        for module in local:
            module_names.append(module.__name__)
        parent_names = []
        for module in parents:
            parent_names.append(module.__name__)
        self.assertEqual([
            'base_internal_internal',
            'base_internal',
            'base'
        ], parent_names)
        self.assertEqual([], module_names)
