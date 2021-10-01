from typing import Iterable

from lzy.api.lazy_op import LzyOp


def print_lzy_op(op: LzyOp) -> None:
    input_types = ", ".join(str(t) for t in op.input_types)
    print(f'{op.func} {op.func.__name__}({input_types}) -> {op.return_type}')


# TODO: remove?
def print_lzy_ops(ops: Iterable[LzyOp]) -> None:
    for lzy_op in ops:
        print_lzy_op(lzy_op)
