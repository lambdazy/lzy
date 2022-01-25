import dataclasses
from typing import Union


@dataclasses.dataclass
class AzureCredentials:
    connection_string: str


@dataclasses.dataclass
class AzureSasCredentials:
    endpoint: str
    signature: str


@dataclasses.dataclass
class AmazonCredentials:
    endpoint: str
    access_token: str
    secret_token: str


StorageCredentials = Union[AzureCredentials, AmazonCredentials, AzureSasCredentials]
