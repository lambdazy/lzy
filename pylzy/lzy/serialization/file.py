import logging
import os
import uuid
from typing import Any, BinaryIO, Callable, Dict, Type, Union, cast

from serialzy.api import StandardDataFormats, Schema, Serializer, StandardSchemaFormats
from lzy.types import File
from lzy.version import __version__
from packaging import version

_LOG = logging.getLogger(__name__)


class FileSerializer(Serializer):
    def serialize(self, obj: File, dest: BinaryIO) -> None:
        with obj.path.open("rb") as f:
            data = f.read(4096)
            while len(data) > 0:
                dest.write(data)
                data = f.read(4096)

    def deserialize(self, source: BinaryIO, typ: Type) -> Any:
        new_path = os.path.join("/tmp", str(uuid.uuid1()))
        with open(new_path, "wb") as f:
            data = source.read(4096)
            while len(data) > 0:
                f.write(data)
                data = source.read(4096)
        return File(new_path)

    def supported_types(self) -> Union[Type, Callable[[Type], bool]]:
        return File

    def available(self) -> bool:
        return True

    def stable(self) -> bool:
        return True

    def data_format(self) -> str:
        return cast(str, StandardDataFormats.raw_file.name)

    def meta(self) -> Dict[str, str]:
        return {"pylzy": __version__}

    def resolve(self, schema: Schema) -> Type:
        if schema.schema_format != StandardSchemaFormats.no_schema.name:
            raise ValueError(f'Invalid schema format {schema.schema_format}')

        if 'pylzy' not in schema.meta:
            _LOG.warning('No pylzy version in meta')
        elif version.parse(schema.meta['pylzy']) > version.parse(__version__):
            _LOG.warning(f'Installed version of pylzy {__version__} '
                         f'is older than used for serialization {schema.meta["pylzy"]}')

        return File

    def schema(self, typ: type) -> Schema:
        if not isinstance(typ, File):
            raise ValueError(f'Only {File} type is supported')
        return Schema(self.data_format(), StandardSchemaFormats.no_schema.name, meta=self.meta())
