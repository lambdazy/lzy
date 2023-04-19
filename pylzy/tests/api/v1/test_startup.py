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

        _, arg_file = tempfile.mkstemp()
        _, kwarg_file = tempfile.mkstemp()
        _, ret_file = tempfile.mkstemp()
        _, data_file = tempfile.mkstemp()
        _, exc_file = tempfile.mkstemp()

        file = File(data_file)
        with open(data_file, "w") as f:
            f.write("2")
        ser = LzySerializerRegistry()

        with open(arg_file, "wb") as arg, open(kwarg_file, "wb") as kwarg:
            ser.find_serializer_by_type(str).serialize("4", arg)
            ser.find_serializer_by_type(File).serialize(file, kwarg)

        startup._lzy_mount = ""

        req = ProcessingRequest(
            get_logging_config(),
            serializers=ser.imports(),
            op=test,
            args_paths=[(str, arg_file)],
            kwargs_paths={"b": (File, kwarg_file)},
            output_paths=[(str, ret_file)],
            exception_path=(Exception, exc_file)
        )

        startup.main(pickle(req))

        with open(ret_file, "rb") as f:
            ret = ser.find_serializer_by_type(str).deserialize(f, str)
            self.assertEqual("42", ret)

    def test_startup_with_exception(self):
        def test() -> None:
            raise ValueError("test")

        _, exc_file = tempfile.mkstemp()
        ser = LzySerializerRegistry()
        startup._lzy_mount = ""

        req = ProcessingRequest(
            get_logging_config(),
            serializers=ser.imports(),
            op=test,
            args_paths=[],
            kwargs_paths={},
            output_paths=[],
            exception_path=(Exception, exc_file)
        )

        original_traceback = None
        try:
            startup.main(pickle(req))
        except ValueError as e:
            original_traceback = e.__traceback__

        with open(exc_file, "rb") as f:
            exc = ser.find_serializer_by_type(Exception).deserialize(f)
            self.assertEqual(ValueError, type(exc))
            self.assertEqual(("test", ), exc.args)
            current_traceback = traceback.extract_tb(exc.__traceback__)
            length = len(current_traceback)
            original_extracted = traceback.extract_tb(original_traceback)[-length:]
            self.assertEqual(original_extracted, current_traceback)
