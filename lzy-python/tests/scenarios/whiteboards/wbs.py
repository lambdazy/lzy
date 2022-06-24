import uuid
from dataclasses import dataclass
from typing import List, Optional

from data import MessageClass, Rule

from lzy.api.v1.whiteboard import view, whiteboard

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
@whiteboard(tags=[simple_whiteboard_tag])
class SimpleWhiteboard:
    a: int = 0
    b: Optional[List[str]] = None

    @view
    def to_simple_view_plus_one_rule(self) -> SimpleView:
        return SimpleView(
            "first_id_SimpleWhiteboard", [Rule(self.a + 1, "plus_one_rule")]
        )

    @view
    def to_simple_view_minus_one_rule(self) -> SimpleView:
        return SimpleView(
            "second_id_SimpleWhiteboard", [Rule(self.a - 1, "minus_one_rule")]
        )

    # it's important that view and staticmethod annotations are present in the following order
    @staticmethod
    @view
    def to_another_simple_view() -> AnotherSimpleView:
        return AnotherSimpleView(
            "first_id_SimpleWhiteboard", "made_from_SimpleWhiteboard"
        )


@dataclass
@whiteboard(namespace="another/simple/whiteboard", tags=[another_simple_whiteboard_tag])
class AnotherSimpleWhiteboard:
    a: str = "first_id_AnotherSimpleWhiteboard"
    b: int = 5
    c: int = 3

    @view
    def to_another_simple_view(self) -> AnotherSimpleView:
        return AnotherSimpleView(self.a, "made_from_AnotherSimpleWhiteboard")


@dataclass
@whiteboard(tags=[simple_whiteboard_tag])
class OneMoreSimpleWhiteboard:
    a: int = 0
    b: Optional[List[str]] = None

    @view
    def to_simple_view_with_plus_two_rule(self) -> SimpleView:
        return SimpleView(
            "third_id_OneMoreSimpleWhiteboard", [Rule(self.a + 2, "plus_two_rule")]
        )


@dataclass
@whiteboard(tags=[default_whiteboard_tag])
class DefaultWhiteboard:
    a: int = 0
    b: Optional[List[str]] = None
    c: str = "Hello"
    d: int = None


@dataclass
@whiteboard(tags=[lzy_message_fields_tag])
class WhiteboardWithLzyMessageFields:
    b: int
    a: MessageClass = MessageClass()


@dataclass
@whiteboard(tags=[lzy_message_fields_tag])
class WhiteboardWithOneLzyMessageField:
    a: MessageClass = MessageClass()


@dataclass
@whiteboard(tags=[lzy_message_fields_tag])
class WhiteboardWithTwoLzyMessageFields:
    a: MessageClass = MessageClass()
    c: MessageClass = MessageClass()
