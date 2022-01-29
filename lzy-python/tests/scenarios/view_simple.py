from dataclasses import dataclass
from typing import List

from lzy.api import op, LzyRemoteEnv
from lzy.api.whiteboard import whiteboard, view
from lzy.servant.terminal_server import TerminalConfig

@dataclass
class Rule:
    a: int
    b: str

class SimpleView:
    rules: List[Rule] = None
    id: str = ''
    def __init__(self, id: str, rules: List[Rule]):
        self.id = id
        self.rules = rules

@dataclass
class AnotherSimpleView:
    id: str
    b: str

@dataclass
@whiteboard(namespace='simple/whiteboard', tags=["simple_whiteboard"])
class SimpleWhiteboard:
    a: int = 0
    b: List[str] = None
    @view
    def toSimpleViewPlusOneRule(self) -> SimpleView:
        return SimpleView('first_id_SimpleWhiteboard', [Rule(self.a + 1, 'plus_one_rule')])
    @view
    def toSimpleViewMinusOneRule(self) -> SimpleView:
        return SimpleView('second_id_SimpleWhiteboard', [Rule(self.a - 1, 'minus_one_rule')])
    # it's important that view and staticmethod annotations are present in the following order
    @view
    @staticmethod
    def toAnotherSimpleView() -> AnotherSimpleView:
        return AnotherSimpleView('first_id_SimpleWhiteboard', 'made_from_SimpleWhiteboard')

@dataclass
@whiteboard(namespace='another/simple/whiteboard', tags=["another_simple_whiteboard"])
class AnotherSimpleWhiteboard:
    a: str = 'first_id_AnotherSimpleWhiteboard'
    b: int = 5
    c: int = 3

    @view
    def toAnotherSimpleView(self) -> AnotherSimpleView:
        return AnotherSimpleView(self.a, 'made_from_AnotherSimpleWhiteboard')

@dataclass
@whiteboard(namespace='simple/whiteboard', tags=["simple_whiteboard"])
class OneMoreSimpleWhiteboard:
    a: int = 0
    b: List[str] = None

    @view
    def toSimpleViewWithPlusTwoRule(self) -> SimpleView:
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
config = TerminalConfig(user="test_user", server_url="localhost:8899")
with LzyRemoteEnv(config=config, whiteboard=wb):
    wb.a = fun1()
    wb.b = fun2(wb.a)


wb = AnotherSimpleWhiteboard()
config = TerminalConfig(user="test_user", server_url="localhost:8899")
with LzyRemoteEnv(config=config, whiteboard=wb):
    wb.a = fun3(3)
    wb.b = fun4(3)
    wb.c = fun5(4)


wb = OneMoreSimpleWhiteboard()
config = TerminalConfig(user="test_user", server_url="localhost:8899")
with LzyRemoteEnv(config=config, whiteboard=wb):
    wb.a = fun1()
    wb.b = fun2(wb.a)

config = TerminalConfig(user="test_user", server_url="localhost:8899")
with LzyRemoteEnv(config=config) as env:
    views = env.whiteboards([SimpleWhiteboard, AnotherSimpleWhiteboard, OneMoreSimpleWhiteboard]).views(SimpleView)
    print("Number of SimpleView views " + str(len(views)))
    simple_view_ids = "Ids of SimpleView "
    simple_view_rules = "Rules of SimpleView "
    for view in views:
        simple_view_ids += view.id + ";"
        simple_view_rules += view.rules[0].b + ";"
    print(simple_view_ids)
    print(simple_view_rules)

    views = env.whiteboards([SimpleWhiteboard, AnotherSimpleWhiteboard, OneMoreSimpleWhiteboard]).views(AnotherSimpleView)
    print("Number of AnotherSimpleView views " + str(len(views)))
    another_simple_view_ids = "Ids of AnotherSimpleView "
    for view in views:
        another_simple_view_ids += view.id + ";"
    print(another_simple_view_ids)

config = TerminalConfig(user="test_user", server_url="localhost:8899")
with LzyRemoteEnv(config=config) as env:
    whiteboards = env.whiteboards([SimpleWhiteboard, AnotherSimpleWhiteboard])
    iteration = "Iterating over whiteboards with types "
    for whiteboard in whiteboards:
        iteration += whiteboard.__class__.__name__ + " "
    print(iteration)
