from unittest import TestCase

from lzy.serialization.registry import LzySerializerRegistry
from tests.serialization.serializer import TestSerializer


class FileSerializationTests(TestCase):
    def setUp(self):
        self.registry = LzySerializerRegistry()

    def test_register(self):
        serializer = TestSerializer()
        self.registry.register_serializer(serializer, 999)
        imports = self.registry.imports()
        self.assertEqual(1, len(imports))
        self.assertEqual("tests.serialization.serializer", imports[0].module_name)
        self.assertEqual("TestSerializer", imports[0].class_name)
        self.assertEqual(999, imports[0].priority)

    def test_load(self):
        serializer = TestSerializer()

        self.registry.register_serializer(serializer, 999)
        imports = self.registry.imports()

        self.registry.unregister_serializer(serializer)
        self.registry.load_imports(imports)
        imports = self.registry.imports()

        self.assertEqual(1, len(imports))
        self.assertEqual("tests.serialization.serializer", imports[0].module_name)
        self.assertEqual("TestSerializer", imports[0].class_name)
        self.assertEqual(999, imports[0].priority)

    def test_load_frim_main_module(self):
        serializer = TestSerializer()
        real_module = type(serializer).__module__
        try:
            type(serializer).__module__ = "__main__"
            with self.assertRaisesRegex(ValueError, "Cannot register serializers from the __main__ module. "
                                                    "Please move serializer class to an external file."):
                self.registry.register_serializer(serializer, 999)
        finally:
            type(serializer).__module__ = real_module
