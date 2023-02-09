import os
from unittest import TestCase

from api.v1.mocks import RuntimeMock, StorageRegistryMock
from lzy.api.v1 import Lzy
from lzy.logs.config import LZY_LOG_LEVEL, get_logger, configure_logging


class LzyLoggingTests(TestCase):
    def test_default_logging(self):
        configure_logging()
        Lzy(runtime=RuntimeMock(), storage_registry=StorageRegistryMock())
        logger = get_logger("lzy.api.v1")
        self.assertEqual(20, logger.parent.level)

    def test_debug_logging(self):
        os.environ[LZY_LOG_LEVEL] = "DEBUG"
        configure_logging()
        Lzy(runtime=RuntimeMock(), storage_registry=StorageRegistryMock())
        logger = get_logger("lzy.api.v1")
        self.assertEqual(10, logger.parent.level)
        del os.environ[LZY_LOG_LEVEL]
