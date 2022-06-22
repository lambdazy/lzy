import uuid

from lzy.api.v1 import op, LzyRemoteEnv
from base_module.base import Base
from some_imported_file_2 import foo


@op
def just_call_imported_stuff() -> str:
    base = Base(1, "before")
    v1, v2 = foo(), base.echo()
    print(v1, v2, sep="\n")
    print("Just print some text")
    return v1 + " " + v2


WORKFLOW_NAME = "workflow_" + str(uuid.uuid4())

with LzyRemoteEnv().workflow(name=WORKFLOW_NAME):
    res = just_call_imported_stuff()

print(res)
