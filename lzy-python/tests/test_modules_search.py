from unittest import TestCase

from lzy.api.pkg_info import select_modules
from tests.test_modules.level1.level1 import Level1


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
        module_names = []
        for module in local:
            module_names.append(module.__name__)

        self.assertEqual(['tests',
                          'tests.test_modules',
                          'tests.test_modules.level1',
                          'tests.test_modules.level1.level2',
                          'tests.test_modules.level1.level2.level3',
                          'tests.test_modules.level1.level2.level3.level3',
                          'tests.test_modules.level1.level2.level2',
                          'tests.test_modules.level1.level1'
                         ],
                         module_names)
        self.assertEqual({"PyYAML", "s3fs"}, set(remote.keys()))
