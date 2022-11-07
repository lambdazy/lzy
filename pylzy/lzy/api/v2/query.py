import dataclasses
import datetime
import typing


@dataclasses.dataclass
class Query:
    name: str
    tags: typing.Sequence[str]
    not_before: typing.Optional[datetime.datetime] = None
    not_after: typing.Optional[datetime.datetime] = None
