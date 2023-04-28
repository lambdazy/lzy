import traceback
from tempfile import NamedTemporaryFile
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

        with NamedTemporaryFile() as arg_file, NamedTemporaryFile() as kwarg_file, NamedTemporaryFile() as ret_file, \
                NamedTemporaryFile() as data_file, NamedTemporaryFile() as exc_file:
            file = File(data_file.name)
            with open(data_file.name, "w") as f:
                f.write("2")
            ser = LzySerializerRegistry()

            with open(arg_file.name, "wb") as arg, open(kwarg_file.name, "wb") as kwarg:
                ser.find_serializer_by_type(str).serialize("4", arg)
                ser.find_serializer_by_type(File).serialize(file, kwarg)

            startup._lzy_mount = ""

            req = ProcessingRequest(
                get_logging_config(),
                serializers=ser.imports(),
                op=test,
                args_paths=[(str, arg_file.name)],
                kwargs_paths={"b": (File, kwarg_file.name)},
                output_paths=[(str, ret_file.name)],
                exception_path=(tuple, exc_file.name)
            )

            startup.main(pickle(req))

            with open(ret_file.name, "rb") as f:
                ret = ser.find_serializer_by_type(str).deserialize(f, str)
                self.assertEqual("42", ret)

    def test_startup_with_exception(self):
        def test() -> None:
            raise ValueError("test")

        with NamedTemporaryFile() as exc_file:
            ser = LzySerializerRegistry()
            startup._lzy_mount = ""

            req = ProcessingRequest(
                get_logging_config(),
                serializers=ser.imports(),
                op=test,
                args_paths=[],
                kwargs_paths={},
                output_paths=[],
                exception_path=(tuple, exc_file.name)
            )

            original_traceback = None
            try:
                startup.main(pickle(req))
            except ValueError as e:
                original_traceback = e.__traceback__

            with open(exc_file.name, "rb") as f:
                exc = ser.find_serializer_by_type(req.exception_path[0]).deserialize(f)
                self.assertEqual(ValueError, type(exc[1]))
                self.assertEqual(("test",), exc[1].args)
                current_traceback = traceback.extract_tb(exc[2])
                length = len(current_traceback)
                original_extracted = traceback.extract_tb(original_traceback)[-length:]
                self.assertEqual(original_extracted, current_traceback)
