from lzy.api.v1 import op, Lzy

from base_module.base import Base, internal_op
from some_imported_file_2 import foo


@op
def just_call_imported_stuff() -> str:
    base = Base(1, "before")
    v1, v2 = foo(), base.echo()
    print(v1, v2, sep="\n")
    print("Just print some text")
    return v1 + " " + v2


if __name__ == '__main__':
    with Lzy().workflow(name="wf", interactive=False):
        res = just_call_imported_stuff()
        print(res)
        internal_op()
