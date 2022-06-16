import uuid
from datetime import datetime
from datetime import timedelta
from typing import List
from typing import Optional

from pure_protobuf.types import int32

from lzy.api.v1 import op, LzyRemoteEnv

from data import Test1, MessageClass, TestEnum
from wbs import (
    AnotherSimpleView,
    SimpleView,
    AnotherSimpleWhiteboard,
    DefaultWhiteboard,
    OneMoreSimpleWhiteboard,
    SimpleWhiteboard,
    WhiteboardWithOneLzyMessageField,
    WhiteboardWithTwoLzyMessageFields,
    WhiteboardWithLzyMessageFields,
)

"""
This scenario contains:
    1. Whiteboards/Views machinery
    2. Custom field serialization scenarios
"""


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


@op
def fun6(a: MessageClass) -> MessageClass:
    list_field: List[int32] = a.list_field
    list_field.append(int32(10))
    optional_field: Optional[int32] = a.optional_field
    optional_field: Optional[int32] = int32(optional_field + 1)
    test: Test1 = a.inner_field
    test.a = test.a + 1
    return MessageClass(
        "fun6:" + a.string_field,
        int32(a.int_field + 1),
        list_field,
        optional_field,
        test,
        TestEnum.BAZ,
    )


@op
def fun7() -> MessageClass:
    return MessageClass(
        "fun7", int32(2), [int32(1), int32(1)], int32(0), Test1(int32(5)), TestEnum.FOO
    )


@op
def fun8(a: MessageClass) -> int:
    return a.int_field


WORKFLOW_NAME = "workflow_" + str(uuid.uuid4())

wb = SimpleWhiteboard()
with LzyRemoteEnv().workflow(name=WORKFLOW_NAME, whiteboard=wb):
    wb.a = fun1()
    wb.b = fun2(wb.a)
    wb_id = wb.__id__

env = LzyRemoteEnv()
# wb = env.whiteboard(wb_id, SimpleWhiteboard)
wb = env.whiteboard_by_id(wb_id)
print(wb.a, wb.a)
print("Len: " + str(len(wb.b)))

wb = env.whiteboard_by_id(wb_id)
print(wb.a, wb.b)
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
views = env.whiteboards(
    [SimpleWhiteboard, AnotherSimpleWhiteboard, OneMoreSimpleWhiteboard]
).views(SimpleView)
print("Number of SimpleView views " + str(len(views)))
simple_view_ids = "Ids of SimpleView "
simple_view_rules = "Rules of SimpleView "
for view in views:
    simple_view_ids += view.id + ";"
    simple_view_rules += view.rules[0].b + ";"
print(simple_view_ids)
print(simple_view_rules)

views = env.whiteboards(
    [SimpleWhiteboard, AnotherSimpleWhiteboard, OneMoreSimpleWhiteboard]
).views(AnotherSimpleView)
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
whiteboards = env.whiteboards(
    [SimpleWhiteboard, AnotherSimpleWhiteboard, OneMoreSimpleWhiteboard],
    from_date=current_datetime_local,
    to_date=next_day_datetime_local,
)
print(
    "Number of whiteboard when date lower and upper bounds are specified is "
    + str(len(whiteboards))
)
whiteboards = env.whiteboards(
    [SimpleWhiteboard, AnotherSimpleWhiteboard, OneMoreSimpleWhiteboard],
    from_date=current_datetime_local,
)
print(
    "Number of whiteboard when date lower bound is specified is "
    + str(len(whiteboards))
)
whiteboards = env.whiteboards(
    [SimpleWhiteboard, AnotherSimpleWhiteboard, OneMoreSimpleWhiteboard],
    to_date=next_day_datetime_local,
)
print(
    "Number of whiteboard when date upper bounds is specified is "
    + str(len(whiteboards))
)
whiteboards = env.whiteboards(
    [SimpleWhiteboard, AnotherSimpleWhiteboard, OneMoreSimpleWhiteboard],
    from_date=next_day_datetime_local,
)
print(
    "Number of whiteboard when date interval is set for the future is "
    + str(len(whiteboards))
)

wb = WhiteboardWithLzyMessageFields(3)
with LzyRemoteEnv().workflow(name=WORKFLOW_NAME, whiteboard=wb):
    wb.a = fun6(fun7())
    wb.b = fun8(wb.a)
    wb_id = wb.__id__

env = LzyRemoteEnv()
# wb = env.whiteboard(wb_id, WhiteboardWithLzyMessageFields)
wb = env.whiteboard_by_id(wb_id)
print("string_field value in WhiteboardWithLzyMessageFields is " + wb.a.string_field)
print("int_field value in WhiteboardWithLzyMessageFields is " + str(wb.a.int_field))
print(
    "list_field length in WhiteboardWithLzyMessageFields is "
    + str(len(wb.a.list_field))
)
print(
    "optional_field value in WhiteboardWithLzyMessageFields is "
    + str(wb.a.optional_field)
)
print(
    "inner_field value in WhiteboardWithLzyMessageFields is " + str(wb.a.inner_field.a)
)
print("enum_field value in WhiteboardWithLzyMessageFields is " + str(wb.a.enum_field))
print("non lzy message int field in WhiteboardWithLzyMessageFields is " + str(wb.b))

# wb = env.whiteboard(wb_id, WhiteboardWithOneLzyMessageField)
wb = env.whiteboard_by_id(wb_id)
print("string_field value in WhiteboardWithOneLzyMessageField is " + wb.a.string_field)
print("int_field value in WhiteboardWithOneLzyMessageField is " + str(wb.a.int_field))

try:
    wb = env.whiteboard(wb_id, WhiteboardWithTwoLzyMessageFields)
    print("Could create WhiteboardWithTwoLzyMessageFields")
except TypeError:
    print(
        "Could not create WhiteboardWithTwoLzyMessageFields because of a missing field"
    )

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
# wb = env.whiteboard(wb_id, DefaultWhiteboard)
wb = env.whiteboard_by_id(wb_id)
print(
    f"Value a in DefaultWhiteboard is {wb.a}, b length is {len(wb.b)}, c is {wb.c}, d is {wb.d}"
)
