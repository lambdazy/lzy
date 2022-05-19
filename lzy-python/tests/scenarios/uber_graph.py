import uuid
from dataclasses import dataclass
from datetime import datetime
from datetime import timedelta
from enum import IntEnum
from typing import List
from typing import Optional

from pure_protobuf.dataclasses_ import field, message
from pure_protobuf.types import int32

from base_module.base import Base
from lzy.v1.api import op
from lzy.v1.api.env import LzyRemoteEnv
from lzy.v1.api.whiteboard import whiteboard, view
from some_imported_file import foo

'''
This scenario contains:
    1. Importing local modules
    2. Functions that return None
    3. Whiteboards/Views machinery
    4. Custom field serialization scenarios
'''


@op
def just_print() -> None:
    base = Base(1, "before")
    print(foo())
    print(base.echo())
    print("Just print some text")


@dataclass
class Rule:
    a: int
    b: str


class SimpleView:
    rules: Optional[List[Rule]] = None
    id: str = ''

    def __init__(self, id_: str, rules: List[Rule]):
        self.id = id_
        self.rules = rules


@dataclass
class AnotherSimpleView:
    id: str
    b: str


simple_whiteboard_tag = "simple_whiteboard_" + str(uuid.uuid4())
another_simple_whiteboard_tag = "another_simple_whiteboard_" + str(uuid.uuid4())
lzy_message_fields_tag = "lzy_message_fields_" + str(uuid.uuid4())
default_whiteboard_tag = "default_whiteboard_" + str(uuid.uuid4())


@dataclass
@whiteboard(tags=[simple_whiteboard_tag])
class SimpleWhiteboard:
    a: int = 0
    b: Optional[List[str]] = None

    @view
    def to_simple_view_plus_one_rule(self) -> SimpleView:
        return SimpleView('first_id_SimpleWhiteboard', [Rule(self.a + 1, 'plus_one_rule')])

    @view
    def to_simple_view_minus_one_rule(self) -> SimpleView:
        return SimpleView('second_id_SimpleWhiteboard', [Rule(self.a - 1, 'minus_one_rule')])

    # it's important that view and staticmethod annotations are present in the following order
    @staticmethod
    @view
    def to_another_simple_view() -> AnotherSimpleView:
        return AnotherSimpleView('first_id_SimpleWhiteboard', 'made_from_SimpleWhiteboard')


@dataclass
@whiteboard(namespace='another/simple/whiteboard', tags=[another_simple_whiteboard_tag])
class AnotherSimpleWhiteboard:
    a: str = 'first_id_AnotherSimpleWhiteboard'
    b: int = 5
    c: int = 3

    @view
    def to_another_simple_view(self) -> AnotherSimpleView:
        return AnotherSimpleView(self.a, 'made_from_AnotherSimpleWhiteboard')


@dataclass
@whiteboard(tags=[simple_whiteboard_tag])
class OneMoreSimpleWhiteboard:
    a: int = 0
    b: Optional[List[str]] = None

    @view
    def to_simple_view_with_plus_two_rule(self) -> SimpleView:
        return SimpleView('third_id_OneMoreSimpleWhiteboard', [Rule(self.a + 2, 'plus_two_rule')])


@dataclass
@whiteboard(tags=[default_whiteboard_tag])
class DefaultWhiteboard:
    a: int = 0
    b: Optional[List[str]] = None
    c: str = "Hello"
    d: int = None


@op
def fun1() -> int:
    return 42


@op
def fun2(a: int) -> List[str]:
    return [str(a), str(a), str(a)]


@op
def fun3(a: int) -> str:
    return str(a)


@op
def fun4(a: int) -> int:
    return a + a


@op
def fun5(a: int) -> int:
    return a * 2


class TestEnum(IntEnum):
    BAR = 1
    FOO = 2
    BAZ = 3


@message
@dataclass
class Test1:
    a: int32 = field(1, default=0)


@message
@dataclass
class MessageClass:
    string_field: str = field(1, default='')
    int_field: int32 = field(2, default=int32(0))
    list_field: List[int32] = field(3, default_factory=list)
    optional_field: Optional[int32] = field(4, default=None)
    inner_field: Test1 = field(5, default_factory=Test1)
    enum_field: TestEnum = field(6, default=TestEnum.BAR)


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


@op
def fun6(a: MessageClass) -> MessageClass:
    list_field: List[int32] = a.list_field
    list_field.append(int32(10))
    optional_field: Optional[int32] = a.optional_field
    optional_field: Optional[int32] = int32(optional_field + 1)
    test: Test1 = a.inner_field
    test.a = test.a + 1
    return MessageClass('fun6:' + a.string_field, int32(a.int_field + 1), list_field, optional_field, test,
                        TestEnum.BAZ)


@op
def fun7() -> MessageClass:
    return MessageClass('fun7', int32(2), [int32(1), int32(1)], int32(0), Test1(int32(5)), TestEnum.FOO)


@op
def fun8(a: MessageClass) -> int:
    return a.int_field


WORKFLOW_NAME = "workflow_" + str(uuid.uuid4())

wb = SimpleWhiteboard()
with LzyRemoteEnv().workflow(name=WORKFLOW_NAME, whiteboard=wb):
    just_print()
    wb.a = fun1()
    wb.b = fun2(wb.a)
    wb_id = wb.__id__

env = LzyRemoteEnv()
wb = env.whiteboard(wb_id, SimpleWhiteboard)
print(wb.a, wb.a)
print("Len: " + str(len(wb.b)))

wb = AnotherSimpleWhiteboard()
with LzyRemoteEnv().workflow(name=WORKFLOW_NAME, whiteboard=wb):
    wb.a = fun3(3)
    wb.b = fun4(3)
    wb.c = fun5(4)

wb = OneMoreSimpleWhiteboard()
with LzyRemoteEnv().workflow(name=WORKFLOW_NAME, whiteboard=wb):
    wb.a = fun1()
    wb.b = fun2(wb.a)

# Simulate crash before whiteboard is finished
wb = OneMoreSimpleWhiteboard()
with LzyRemoteEnv().workflow(name=WORKFLOW_NAME, whiteboard=wb) as env:
    wb.a = fun1()
    wb.b = fun2(wb.a)
    # noinspection PyProtectedMember
    env._ops.clear()

env = LzyRemoteEnv()
views = env.whiteboards([SimpleWhiteboard, AnotherSimpleWhiteboard, OneMoreSimpleWhiteboard]).views(SimpleView)
print("Number of SimpleView views " + str(len(views)))
simple_view_ids = "Ids of SimpleView "
simple_view_rules = "Rules of SimpleView "
for view in views:
    simple_view_ids += view.id + ";"
    simple_view_rules += view.rules[0].b + ";"
print(simple_view_ids)
print(simple_view_rules)

views = env.whiteboards([SimpleWhiteboard, AnotherSimpleWhiteboard, OneMoreSimpleWhiteboard]).views(
    AnotherSimpleView)
print("Number of AnotherSimpleView views " + str(len(views)))
another_simple_view_ids = "Ids of AnotherSimpleView "
for view in views:
    another_simple_view_ids += view.id + ";"
print(another_simple_view_ids)

whiteboards = env.whiteboards([SimpleWhiteboard, AnotherSimpleWhiteboard])
print("Number of whiteboard is " + str(len(whiteboards)))
print("First whiteboard type is " + whiteboards[0].__class__.__name__)
iteration = "Iterating over whiteboards with types "
for whiteboard in whiteboards:
    iteration += whiteboard.__class__.__name__ + " "
print(iteration)

current_datetime_local = datetime.now() - timedelta(days=1)
next_day_datetime_local = current_datetime_local + timedelta(days=1)
whiteboards = env.whiteboards([SimpleWhiteboard, AnotherSimpleWhiteboard, OneMoreSimpleWhiteboard],
                              from_date=current_datetime_local, to_date=next_day_datetime_local)
print("Number of whiteboard when date lower and upper bounds are specified is " + str(len(whiteboards)))
whiteboards = env.whiteboards([SimpleWhiteboard, AnotherSimpleWhiteboard, OneMoreSimpleWhiteboard],
                              from_date=current_datetime_local)
print("Number of whiteboard when date lower bound is specified is " + str(len(whiteboards)))
whiteboards = env.whiteboards([SimpleWhiteboard, AnotherSimpleWhiteboard, OneMoreSimpleWhiteboard],
                              to_date=next_day_datetime_local)
print("Number of whiteboard when date upper bounds is specified is " + str(len(whiteboards)))
whiteboards = env.whiteboards([SimpleWhiteboard, AnotherSimpleWhiteboard, OneMoreSimpleWhiteboard],
                              from_date=next_day_datetime_local)
print("Number of whiteboard when date interval is set for the future is " + str(len(whiteboards)))

wb = WhiteboardWithLzyMessageFields(3)
with LzyRemoteEnv().workflow(name=WORKFLOW_NAME, whiteboard=wb):
    wb.a = fun6(fun7())
    wb.b = fun8(wb.a)
    wb_id = wb.__id__

env = LzyRemoteEnv()
wb = env.whiteboard(wb_id, WhiteboardWithLzyMessageFields)
print("string_field value in WhiteboardWithLzyMessageFields is " + wb.a.string_field)
print("int_field value in WhiteboardWithLzyMessageFields is " + str(wb.a.int_field))
print("list_field length in WhiteboardWithLzyMessageFields is " + str(len(wb.a.list_field)))
print("optional_field value in WhiteboardWithLzyMessageFields is " + str(wb.a.optional_field))
print("inner_field value in WhiteboardWithLzyMessageFields is " + str(wb.a.inner_field.a))
print("enum_field value in WhiteboardWithLzyMessageFields is " + str(wb.a.enum_field))
print("non lzy message int field in WhiteboardWithLzyMessageFields is " + str(wb.b))

wb = env.whiteboard(wb_id, WhiteboardWithOneLzyMessageField)
print("string_field value in WhiteboardWithOneLzyMessageField is " + wb.a.string_field)
print("int_field value in WhiteboardWithOneLzyMessageField is " + str(wb.a.int_field))

try:
    wb = env.whiteboard(wb_id, WhiteboardWithTwoLzyMessageFields)
    print("Could create WhiteboardWithTwoLzyMessageFields")
except TypeError:
    print("Could not create WhiteboardWithTwoLzyMessageFields because of a missing field")

wb = WhiteboardWithOneLzyMessageField()
with LzyRemoteEnv().workflow(name=WORKFLOW_NAME, whiteboard=wb):
    wb.a = fun7()
    wb_id = wb.__id__

env = LzyRemoteEnv()
try:
    wb = env.whiteboard(wb_id, WhiteboardWithLzyMessageFields)
    print("Could create WhiteboardWithLzyMessageFields")
except TypeError:
    print("Could not create WhiteboardWithLzyMessageFields because of a missing field")

wb = DefaultWhiteboard()
with LzyRemoteEnv().workflow(name=WORKFLOW_NAME, whiteboard=wb):
    wb.a = 7
    wb.b = fun2(fun1())
    wb_id = wb.__id__

env = LzyRemoteEnv()
wb = env.whiteboard(wb_id, DefaultWhiteboard)
print(f"Value a in DefaultWhiteboard is {wb.a}, b length is {len(wb.b)}, c is {wb.c}, d is {wb.d}")