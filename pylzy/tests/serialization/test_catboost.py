import tempfile
from unittest import TestCase

# noinspection PyPackageRequirements
from catboost import Pool

from lzy.serialization.catboost import CatboostPoolSerializer
from lzy.serialization.registry import DefaultSerializerRegistry


class CatboostSerializationTests(TestCase):
    def setUp(self):
        self.registry = DefaultSerializerRegistry()

    def test_catboost_pool_serialization(self):
        pool = Pool(
            data=[[1, 4, 5, 6], [4, 5, 6, 7], [30, 40, 50, 60]],
            label=[1, 1, -1],
            weight=[0.1, 0.2, 0.3],
        )
        serializer = self.registry.find_serializer_by_type(type(pool))
        with tempfile.TemporaryFile() as file:
            serializer.serialize(pool, file)
            file.flush()
            file.seek(0)
            deserialized_pool = serializer.deserialize(file, Pool)

        self.assertTrue(isinstance(serializer, CatboostPoolSerializer))
        self.assertEqual(pool.get_weight(), deserialized_pool.get_weight())
        self.assertTrue(serializer.stable())
        self.assertIn("catboost", serializer.meta())
