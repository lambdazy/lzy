import uuid
from dataclasses import dataclass
from typing import List, Optional

from data import MessageClass, Rule
from lzy.api.v1 import whiteboard

simple_whiteboard_tag = "simple_whiteboard_" + str(uuid.uuid4())
another_simple_whiteboard_tag = "another_simple_whiteboard_" + str(uuid.uuid4())
lzy_message_fields_tag = "lzy_message_fields_" + str(uuid.uuid4())
default_whiteboard_tag = "default_whiteboard_" + str(uuid.uuid4())


class SimpleView:
    rules: Optional[List[Rule]] = None
    id: str = ""

    def __init__(self, id_: str, rules: List[Rule]):
        self.id = id_
        self.rules = rules


@dataclass
class AnotherSimpleView:
    id: str
    b: str


@dataclass
@whiteboard("SimpleWhiteboard")
class SimpleWhiteboard:
    a: int = 0
    b: Optional[List[str]] = None


@dataclass
@whiteboard("AnotherSimpleWhiteboard")
class AnotherSimpleWhiteboard:
    a: str = "first_id_AnotherSimpleWhiteboard"
    b: int = 5
    c: int = 3


@dataclass
@whiteboard("OneMoreSimpleWhiteboard")
class OneMoreSimpleWhiteboard:
    a: int = 0
    b: Optional[List[str]] = None


@dataclass
@whiteboard("DefaultWhiteboard")
class DefaultWhiteboard:
    a: int = 0
    b: Optional[List[str]] = None
    c: str = "Hello"
    d: Optional[int] = None


@dataclass
@whiteboard("WhiteboardWithLzyMessageFields")
class WhiteboardWithLzyMessageFields:
    b: int
    a: MessageClass = MessageClass()


@dataclass
@whiteboard("WhiteboardWithOneLzyMessageField")
class WhiteboardWithOneLzyMessageField:
    a: MessageClass = MessageClass()


@dataclass
@whiteboard("WhiteboardWithTwoLzyMessageFields")
class WhiteboardWithTwoLzyMessageFields:
    a: MessageClass = MessageClass()
    c: MessageClass = MessageClass()
