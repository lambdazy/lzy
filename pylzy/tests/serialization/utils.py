import tempfile
from typing import Any

from lzy.serialization.api import SerializerRegistry


def serialized_and_deserialized(registry: SerializerRegistry, var: Any) -> Any:
    serializer = registry.find_serializer_by_type(type(var))
    with tempfile.TemporaryFile() as file:
        serializer.serialize(var, file)
        file.flush()
        file.seek(0)
        deserialized = serializer.deserialize(file, type(var))
    return deserialized
