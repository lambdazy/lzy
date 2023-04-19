from inspect import isclass
from typing import Any, BinaryIO, Callable, Dict, Type, Union, Optional

from packaging import version
from serialzy.api import Schema, StandardSchemaFormats, VersionBoundary
from serialzy.base import DefaultSchemaSerializerByValue

from lzy.logs.config import get_logger
from lzy.types import File
from lzy.version import __version__

_LOG = get_logger(__name__)


class ExceptionSerializer(DefaultSchemaSerializerByValue):
    DATA_FORMAT = "raw_exception"

    def _serialize(self, obj: File, dest: BinaryIO) -> None:
        from tblib import pickling_support  # type: ignore
        pickling_support.install()
        import pickle  # type: ignore
        pickle.dump(obj, dest)

    def _deserialize(self, source: BinaryIO, schema_type: Type, user_type: Optional[Type] = None) -> Any:
        self._check_types_valid(schema_type, user_type)
        import pickle  # type: ignore
        return pickle.load(source)

    def supported_types(self) -> Union[Type, Callable[[Type], bool]]:
        return lambda x: isclass(x) and issubclass(x, Exception)

    def available(self) -> bool:
        base_available = super().available()
        if not base_available:
            return False
        # noinspection PyBroadException
        try:
            import tblib  # type: ignore
            return True
        except Exception:
            return False

    def stable(self) -> bool:
        return False

    def data_format(self) -> str:
        return self.DATA_FORMAT

    def meta(self) -> Dict[str, str]:
        import tblib
        return {"pylzy": __version__, "tblib": tblib.__version__}

    def resolve(self, schema: Schema) -> Type:
        if 'pylzy' not in schema.meta:
            _LOG.warning('No pylzy version in meta')
        elif version.parse(schema.meta['pylzy']) > version.parse(__version__):
            _LOG.warning(f'Installed version of pylzy {__version__} '
                         f'is older than used for serialization {schema.meta["pylzy"]}')
        if 'tblib' not in schema.meta:
            _LOG.warning('No tblib version in meta')
        elif version.parse(schema.meta['tblib']) > version.parse(__version__):
            _LOG.warning(f'Installed version of tblib {__version__} '
                         f'is older than used for serialization {schema.meta["tblib"]}')
        return Exception

    def schema(self, typ: type) -> Schema:
        if not issubclass(typ, Exception):
            raise ValueError(f'Invalid type {typ}')
        return Schema(self.data_format(), StandardSchemaFormats.no_schema.name, meta=self.meta())

    def requirements(self) -> Dict[str, VersionBoundary]:
        return {"tblib": VersionBoundary()}
