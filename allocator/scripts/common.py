from typing import Type, Any

import sys
import logging
from yaml import load, SafeLoader
from marshmallow_dataclass import class_schema
import os
import yandexcloud


def strict_load_yaml(yaml: str, loaded_type: Type[Any]):
    schema = class_schema(loaded_type)
    return schema().load(load(yaml, Loader=SafeLoader))


def create_sdk():
    token = os.getenv("YC_TOKEN", default="")
    if token is None or token == "":
        raise Exception("Expected authorization token in YC_TOKEN env variable!\nExecute this in terminal:\n\nexport "
                        "YC_TOKEN=$(yc iam create-token)\n")
    else:
        return yandexcloud.SDK(iam_token=token)


def format_logs():
    root = logging.getLogger()
    root.setLevel(logging.DEBUG)
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s  - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    root.addHandler(handler)


def gigabytes_to_bytes(gbs: int):
    return gbs * (1024 ** 3)
