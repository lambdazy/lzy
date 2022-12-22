import uuid
from datetime import datetime, timedelta
from typing import List, Optional

from pure_protobuf.types import int32

from data import MessageClass, Test1, TestEnum
from lzy.api.v1 import Lzy, op
from wbs import (
    AnotherSimpleView,
    AnotherSimpleWhiteboard,
    DefaultWhiteboard,
    OneMoreSimpleWhiteboard,
    SimpleView,
    SimpleWhiteboard,
    WhiteboardWithLzyMessageFields,
    WhiteboardWithOneLzyMessageField,
    WhiteboardWithTwoLzyMessageFields, simple_whiteboard_tag, another_simple_whiteboard_tag, lzy_message_fields_tag,
    default_whiteboard_tag,
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


WORKFLOW_NAME = "wf"

lzy = Lzy()

with lzy.workflow(name=WORKFLOW_NAME, interactive=False) as wf:
    wb = wf.create_whiteboard(SimpleWhiteboard, tags=[simple_whiteboard_tag])
    wb.a = fun1()
    wb.b = fun2(42)
    wb_id = wb.id

wb = lzy.whiteboard(wb_id)
print(wb.a, wb.a)
print(f"Len: {len(wb.b)}")

wb = lzy.whiteboard(wb_id)
print(wb.a, wb.b)
print(f"Len: {len(wb.b)}")


with lzy.workflow(name=WORKFLOW_NAME, interactive=False) as wf:
    wb = wf.create_whiteboard(AnotherSimpleWhiteboard, tags=[another_simple_whiteboard_tag])
    wb.a = fun3(3)
    wb.b = fun4(3)
    wb.c = fun5(4)


with lzy.workflow(name=WORKFLOW_NAME, interactive=False) as wf:
    wb = wf.create_whiteboard(OneMoreSimpleWhiteboard, tags=[simple_whiteboard_tag])
    wb.a = fun1()
    wb.b = fun2(wb.a)

whiteboards = lzy.whiteboards(tags=[simple_whiteboard_tag])
print("Number of whiteboard with simple_whiteboard_tag is " + str(len(whiteboards)))

current_datetime_local = datetime.now() - timedelta(days=1)
next_day_datetime_local = current_datetime_local + timedelta(days=1)
whiteboards = lzy.whiteboards(
    not_before=current_datetime_local,
    not_after=next_day_datetime_local,
)
print(
    "Number of whiteboard when date lower and upper bounds are specified is "
    + str(len(whiteboards))
)
whiteboards = lzy.whiteboards(
    not_before=current_datetime_local,
)
print(
    "Number of whiteboard when date lower bound is specified is "
    + str(len(whiteboards))
)
whiteboards = lzy.whiteboards(
    not_after=next_day_datetime_local,
)
print(
    "Number of whiteboard when date upper bounds is specified is "
    + str(len(whiteboards))
)
whiteboards = lzy.whiteboards(
    not_before=next_day_datetime_local
)
print(
    "Number of whiteboard when date interval is set for the future is "
    + str(len(whiteboards))
)

with lzy.workflow(name=WORKFLOW_NAME, interactive=False) as wf:
    wb = wf.create_whiteboard(WhiteboardWithLzyMessageFields, tags=[lzy_message_fields_tag])
    wb.a = fun6(fun7())
    wb.b = fun8(wb.a)
    wb_id = wb.id

wb = lzy.whiteboard(wb_id)
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


wb = lzy.whiteboard(wb_id)
print("string_field value in WhiteboardWithOneLzyMessageField is " + wb.a.string_field)
print("int_field value in WhiteboardWithOneLzyMessageField is " + str(wb.a.int_field))

with lzy.workflow(name=WORKFLOW_NAME, interactive=False) as wf:
    wb = wf.create_whiteboard(WhiteboardWithOneLzyMessageField, tags=[lzy_message_fields_tag])
    wb.a = fun7()

with lzy.workflow(name=WORKFLOW_NAME, interactive=False) as wf:
    wb = wf.create_whiteboard(DefaultWhiteboard, tags=[default_whiteboard_tag])
    wb.a = 7
    wb.b = fun2(fun1())
    wb_id = wb.id

wb = lzy.whiteboard(wb_id)
print(
    f"Value a in DefaultWhiteboard is {wb.a}, b length is {len(wb.b)}, c is {wb.c}, d is {wb.d}"
)

with lzy.workflow(name=WORKFLOW_NAME, interactive=False) as wf:
    wb = wf.create_whiteboard(WhiteboardWithOneLzyMessageField, tags=[lzy_message_fields_tag])
    wb.a = MessageClass(
        "local_data",
        int32(2),
        [int32(1), int32(1)],
        int32(0),
        Test1(int32(5)),
        TestEnum.FOO,
    )
    wb_id = wb.id

wb = lzy.whiteboard(wb_id)
print(
    f"Value a.string_field in WhiteboardWithOneLzyMessageField is {wb.a.string_field}"
)
