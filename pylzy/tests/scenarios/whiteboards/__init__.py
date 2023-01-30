from dataclasses import dataclass
from datetime import datetime, timedelta

from lzy.api.v1 import Lzy, op, whiteboard

"""
This scenario contains:
    1. Whiteboard machinery
    2. Custom field serialization scenarios
"""


@dataclass
@whiteboard("SimpleWhiteboard")
class SimpleWhiteboard:
    a: int
    b: str = "str"


@op
def returns_int() -> int:
    return 42


@op
def returns_str() -> str:
    return "new_str"


lzy = Lzy()
with lzy.workflow(name="wf", interactive=False) as wf:
    wb = wf.create_whiteboard(SimpleWhiteboard, tags=["wf_tag"])
    wb.b = returns_str()
    a = returns_int()
    print(a)
    wb.a = a

whiteboard = lzy.whiteboard(id_=wb.id)
print(f"Attempt 1: Value wb.a: {whiteboard.a}")
print(f"Attempt 1: Value wb.b: {whiteboard.b}")

lzy = Lzy()
with lzy.workflow(name="wf2", interactive=False) as wf:
    wb2 = wf.create_whiteboard(SimpleWhiteboard)
    wb2.a = returns_int()

whiteboard = lzy.whiteboard(id_=wb.id)
whiteboard2 = lzy.whiteboard(storage_uri=wb2.storage_uri)
print(f"Attempt 2: Value wb.a: {whiteboard.a}")
print(f"Attempt 2: Value wb.b: {whiteboard.b}")
print(f"Attempt 2: Value wb2.a: {whiteboard2.a}")
print(f"Attempt 2: Value wb2.b: {whiteboard2.b}")

lzy = Lzy()
whiteboards = list(lzy.whiteboards(
    name="SimpleWhiteboard"
))
print(
    "Number of whiteboard when name is specified is "
    + str(len(whiteboards)))

whiteboards = list(lzy.whiteboards(
    tags=["wf_tag"]
))
print(
    "Number of whiteboard when tag is specified is "
    + str(len(whiteboards)))

prev_day_datetime_local = datetime.now() - timedelta(days=1)
next_day_datetime_local = prev_day_datetime_local + timedelta(days=1)
whiteboards = list(lzy.whiteboards(
    not_before=prev_day_datetime_local,
    not_after=next_day_datetime_local,
))
print(
    "Number of whiteboard when date lower and upper bounds are specified is "
    + str(len(whiteboards))
)

whiteboards = list(lzy.whiteboards(
    name="ComplexWhiteboard"
))
print(
    "Number of whiteboard when invalid name is specified is "
    + str(len(whiteboards)))

whiteboards = list(lzy.whiteboards(
    tags=["not_wf_tag"]
))
print(
    "Number of whiteboard when invalid tag is specified is "
    + str(len(whiteboards)))

whiteboards = list(lzy.whiteboards(
    not_before=next_day_datetime_local,
    not_after=prev_day_datetime_local,
))
print(
    "Number of whiteboard when date lower and upper bounds are invalid is "
    + str(len(whiteboards)))
