import uuid
from dataclasses import dataclass
from typing import List
from datetime import datetime
from datetime import timedelta

from lzy.api import op, LzyRemoteEnv
from lzy.api.whiteboard import whiteboard, view

from base import Base

'''
This scenario contains:
    1. Importing local modules
    2. Functions that return None
    3. Whiteboards/Views machinery
'''


@op
def just_print() -> None:
    base = Base(1, "before")
    print(base.echo())
    print("Just print some text")


@dataclass
class Rule:
    a: int
    b: str


class SimpleView:
    rules: List[Rule] = None
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


@dataclass
@whiteboard(tags=[simple_whiteboard_tag])
class SimpleWhiteboard:
    a: int = 0
    b: List[str] = None

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
    b: List[str] = None

    @view
    def to_simple_view_with_plus_two_rule(self) -> SimpleView:
        return SimpleView('third_id_OneMoreSimpleWhiteboard', [Rule(self.a + 2, 'plus_two_rule')])


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


wb = SimpleWhiteboard()
with LzyRemoteEnv(whiteboard=wb):
    just_print()
    wb.a = fun1()
    wb.b = fun2(wb.a)
    wb_id = wb.__id__

with LzyRemoteEnv() as env:
    wb = env.whiteboard(wb_id, SimpleWhiteboard)
    print(wb.a, wb.a)
    print("Len: " + str(len(wb.b)))

wb = AnotherSimpleWhiteboard()
with LzyRemoteEnv(whiteboard=wb):
    wb.a = fun3(3)
    wb.b = fun4(3)
    wb.c = fun5(4)

wb = OneMoreSimpleWhiteboard()
with LzyRemoteEnv(whiteboard=wb):
    wb.a = fun1()
    wb.b = fun2(wb.a)

# Simulate crash before whiteboard is finished
wb = OneMoreSimpleWhiteboard()
with LzyRemoteEnv(whiteboard=wb) as env:
    wb.a = fun1()
    wb.b = fun2(wb.a)
    # noinspection PyProtectedMember
    env._ops.clear()

with LzyRemoteEnv() as env:
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

with LzyRemoteEnv() as env:
    whiteboards = env.whiteboards([SimpleWhiteboard, AnotherSimpleWhiteboard])
    print("Number of whiteboard is " + str(len(whiteboards)))
    print("First whiteboard type is " + whiteboards[0].__class__.__name__)
    iteration = "Iterating over whiteboards with types "
    for whiteboard in whiteboards:
        iteration += whiteboard.__class__.__name__ + " "
    print(iteration)

with LzyRemoteEnv() as env:
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



