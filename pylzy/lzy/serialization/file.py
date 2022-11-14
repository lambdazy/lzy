import logging
import os
import uuid
from typing import Any, BinaryIO, Callable, Dict, Type, Union

from lzy.serialization.api import DefaultDataSchemaSerializer, StandardDataFormats, Schema
from lzy.serialization.types import File
from lzy.serialization.utils import cached_installed_packages
from lzy.version import __version__
from packaging import version

_LOG = logging.getLogger(__name__)


class FileSerializer(DefaultDataSchemaSerializer):
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

    def format(self) -> str:
        return StandardDataFormats.raw_file.name

    def meta(self) -> Dict[str, str]:
        return {"pylzy": __version__}

    def resolve(self, schema: Schema) -> Type:
        typ = super().resolve(schema)
        if 'pylzy' not in schema.meta:
            _LOG.warning('No pylzy version in meta')
        elif version.parse(schema.meta['pylzy']) > version.parse(cached_installed_packages["pylzy"]):
            _LOG.warning(f'Installed version of pylzy {cached_installed_packages["pylzy"]} '
                         f'is older than used for serialization {schema.meta["pylzy"]}')
        return typ
