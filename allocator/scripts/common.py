from typing import Type, Any

from yaml import load, SafeLoader
from marshmallow_dataclass import class_schema


def strict_load_yaml(yaml: str, loaded_type: Type[Any]):
    schema = class_schema(loaded_type)
    return schema().load(load(yaml, Loader=SafeLoader))
