from enum import Enum


class ReturnCode(Enum):
    ENVIRONMENT_INSTALLATION_ERROR = 97
    EXECUTION_ERROR = 98


class PyReturnCode(Enum):
    DESERIALIZATION_FAILURE = 151
