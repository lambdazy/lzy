import logging
from typing import Any, BinaryIO, Callable, Dict, Type, Union

import cloudpickle

from lzy.serialization.api import DefaultDataSchemaSerializer, StandardDataFormats, Schema
from packaging import version

from lzy.serialization.utils import cached_installed_packages

_LOG = logging.getLogger(__name__)


# noinspection PyMethodMayBeStatic
class CloudpickleSerializer(DefaultDataSchemaSerializer):
    def serialize(self, obj: Any, dest: BinaryIO) -> None:
        cloudpickle.dump(obj, dest)

    def deserialize(self, source: BinaryIO, typ: Type) -> Any:
        return cloudpickle.load(source)

    def available(self) -> bool:
        return True

    def stable(self) -> bool:
        return False

    def supported_types(self) -> Union[Type, Callable[[Type], bool]]:
        return lambda x: True

    def format(self) -> str:
        return StandardDataFormats.pickle.name

    def meta(self) -> Dict[str, str]:
        return {"cloudpickle": cloudpickle.__version__}

    def resolve(self, schema: Schema) -> Type:
        typ = super().resolve(schema)
        if 'cloudpickle' not in schema.meta:
            _LOG.warning('No cloudpickle version in meta')
        elif version.parse(schema.meta['cloudpickle']) > version.parse(cached_installed_packages["cloudpickle"]):
            _LOG.warning(f'Installed version of cloudpickle {cached_installed_packages["cloudpickle"]} '
                         f'is older than used for serialization {schema.meta["cloudpickle"]}')
        return typ
