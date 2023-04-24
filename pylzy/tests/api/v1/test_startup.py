import os
import tempfile
import traceback
from unittest import TestCase

from lzy.api.v1.startup import ProcessingRequest
from lzy.api.v1.utils.pickle import pickle
from lzy.logs.config import get_logging_config

from lzy.api.v1 import startup

from lzy.serialization.registry import LzySerializerRegistry
from lzy.types import File


class StartupTests(TestCase):
    def test_startup(self):
        # noinspection PyShadowingNames
        def test(a: str, *, b: File) -> str:
            # noinspection PyShadowingNames
            with b.open("r") as f:
                return a + f.readline()

        arg_file, arg_file_name = tempfile.mkstemp()
        kwarg_file, kwarg_file_name = tempfile.mkstemp()
        ret_file, ret_file_name = tempfile.mkstemp()
        data_file, data_file_name = tempfile.mkstemp()
        exc_file, exc_file_name = tempfile.mkstemp()

        file = File(data_file_name)
        with open(data_file_name, "w") as f:
            f.write("2")
        ser = LzySerializerRegistry()

        with open(arg_file_name, "wb") as arg, open(kwarg_file_name, "wb") as kwarg:
            ser.find_serializer_by_type(str).serialize("4", arg)
            ser.find_serializer_by_type(File).serialize(file, kwarg)

        startup._lzy_mount = ""

        req = ProcessingRequest(
            get_logging_config(),
            serializers=ser.imports(),
            op=test,
            args_paths=[(str, arg_file_name)],
            kwargs_paths={"b": (File, kwarg_file_name)},
            output_paths=[(str, ret_file_name)],
            exception_path=(Exception, exc_file_name)
        )

        startup.main(pickle(req))

        with open(ret_file_name, "rb") as f:
            ret = ser.find_serializer_by_type(str).deserialize(f, str)
            self.assertEqual("42", ret)

        os.close(arg_file)
        os.close(kwarg_file)
        os.close(ret_file)
        os.close(data_file)
        os.close(exc_file)

    def test_startup_with_exception(self):
        def test() -> None:
            raise ValueError("test")

        exc_file, exc_file_name = tempfile.mkstemp()
        ser = LzySerializerRegistry()
        startup._lzy_mount = ""

        req = ProcessingRequest(
            get_logging_config(),
            serializers=ser.imports(),
            op=test,
            args_paths=[],
            kwargs_paths={},
            output_paths=[],
            exception_path=(Exception, exc_file_name)
        )

        original_traceback = None
        try:
            startup.main(pickle(req))
        except ValueError as e:
            original_traceback = e.__traceback__

        with open(exc_file_name, "rb") as f:
            exc = ser.find_serializer_by_type(Exception).deserialize(f)
            self.assertEqual(ValueError, type(exc[1]))
            self.assertEqual(("test", ), exc[1].args)
            current_traceback = traceback.extract_tb(exc[2])
            length = len(current_traceback)
            original_extracted = traceback.extract_tb(original_traceback)[-length:]
            self.assertEqual(original_extracted, current_traceback)

        os.close(exc_file)
