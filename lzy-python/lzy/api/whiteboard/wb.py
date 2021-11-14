from collections import defaultdict
from typing import Any, Optional

from lzy.api import is_lazy_proxy, LzyOp


class WhiteBoard:
    __ignore__ = ['deps', 'ops', 'name']

    def __init__(self):
        self.deps = defaultdict(set)
        self.ops = {}
        # self.name = name
        # add id for WB

    def __setattr__(self, key, value):
        if key in WhiteBoard.__ignore__:
            return super().__setattr__(key, value)

        if is_lazy_proxy(value):
            self.ops[key] = value
            self.__register_dep(key, value)
        # else:
        #     raise AttributeError(f'{key}: {value} is not lazy proxy')

    def __getattr__(self, item):
        if item in self.ops:
            return self.ops[item]
        raise AttributeError(f'No such attribute')

    def __register_dep(self, key: str, lzy_proxy):
        # noinspection PyProtectedMember
        original_lazy_op = lzy_proxy._op  # type: LzyOp
        for arg in original_lazy_op.args:
            if not is_lazy_proxy(arg):
                continue

            dep_name = self.find_arg_key(arg)
            if dep_name is None:
                self.__register_dep(key, arg)
            else:
                self.deps[key].add(dep_name)
                self.deps[key].update(self.deps[dep_name])

    def find_arg_key(self, arg: Any) -> Optional[str]:
        for key, op in self.ops.items():
            if op is arg:
                return key
        return None
