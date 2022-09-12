from typing import Type, Any

from yaml import load, SafeLoader
from marshmallow_dataclass import class_schema
import os
import yandexcloud


def strict_load_yaml(yaml: str, loaded_type: Type[Any]):
    schema = class_schema(loaded_type)
    return schema().load(load(yaml, Loader=SafeLoader))


def create_sdk():
    token = os.environ['YC_TOKEN']
    if token is None or token == "":
        raise Exception("Expected authorization token in YC_TOKEN env variable!\nExecute this in terminal:\n\nexport "
                        "YC_TOKEN=$(yc iam create-token)\n")
    else:
        return yandexcloud.SDK(iam_token=token)

