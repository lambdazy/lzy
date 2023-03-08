import os
import uuid
from typing import Any, BinaryIO, Callable, Dict, Type, Union, Optional

from packaging import version
from serialzy.api import Schema, StandardSchemaFormats, VersionBoundary
from serialzy.base import DefaultSchemaSerializerByReference

from lzy.logs.config import get_logger
from lzy.types import File
from lzy.version import __version__

_LOG = get_logger(__name__)


class FileSerializer(DefaultSchemaSerializerByReference):
    DATA_FORMAT = "raw_file"
    PERMISSIONS_HEADER = "permissions".encode("utf-8")

    def _serialize(self, obj: File, dest: BinaryIO) -> None:
        stat = obj.stat()
        permissions = oct(stat.st_mode & 0o777)
        with obj.open("rb") as f:
            dest.write(self.PERMISSIONS_HEADER)
            dest.write(permissions.encode("utf-8"))

            data = f.read(4096)
            while len(data) > 0:
                dest.write(data)
                data = f.read(4096)

    def _deserialize(self, source: BinaryIO, schema_type: Type, user_type: Optional[Type] = None) -> Any:
        self._check_types_valid(schema_type, user_type)
        new_path = os.path.join("/tmp", str(uuid.uuid1()))
        permissions: Optional[str] = None
        with open(new_path, "wb") as f:
            header = source.read(11)
            if header == self.PERMISSIONS_HEADER:
                permissions = source.read(5).decode("utf-8")
            elif len(header) > 0:
                f.write(header)  # for compatibility with files without header

            data = source.read(4096)
            while len(data) > 0:
                f.write(data)
                data = source.read(4096)
        file = File(new_path)
        if permissions:
            file.chmod(int(permissions, 8))
        return file

    def supported_types(self) -> Union[Type, Callable[[Type], bool]]:
        return File

    def available(self) -> bool:
        return True

    def stable(self) -> bool:
        return True

    def data_format(self) -> str:
        return self.DATA_FORMAT

    def meta(self) -> Dict[str, str]:
        return {"pylzy": __version__}

    def resolve(self, schema: Schema) -> Type:
        if schema.data_format != self.DATA_FORMAT:
            raise ValueError(f'Invalid data format {schema.data_format}')
        if schema.schema_format != StandardSchemaFormats.no_schema.name:
            raise ValueError(f'Invalid schema format {schema.schema_format}')

        if 'pylzy' not in schema.meta:
            _LOG.warning('No pylzy version in meta')
        elif version.parse(schema.meta['pylzy']) > version.parse(__version__):
            _LOG.warning(f'Installed version of pylzy {__version__} '
                         f'is older than used for serialization {schema.meta["pylzy"]}')

        return File

    def schema(self, typ: type) -> Schema:
        if typ != File:
            raise ValueError(f'Invalid type {typ}')
        return Schema(self.data_format(), StandardSchemaFormats.no_schema.name, meta=self.meta())

    def requirements(self) -> Dict[str, VersionBoundary]:
        return {}
