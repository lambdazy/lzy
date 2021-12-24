import dataclasses


@dataclasses.dataclass
class AzureCredentials:
    connection_string: str


@dataclasses.dataclass
class AmazonCredentials:
    endpoint: str
    access_token: str
    secret_token: str
