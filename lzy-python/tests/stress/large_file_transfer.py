from dataclasses import dataclass
import numpy as np
import uuid

from lzy.api import op, LzyRemoteEnv
from lzy.api.whiteboard import whiteboard


simple_whiteboard_tag = "simple_whiteboard_" + str(uuid.uuid4())


@dataclass
@whiteboard(tags=[simple_whiteboard_tag])
class SimpleWhiteboard:
    data: bytes = b"Hello world"


@op
def assert_big_data(real_size, data) -> str:
    assert len(data) == real_size
    return data


def get_data(size) -> bytes:
    return np.random.randint(0, 10, size, dtype=np.int8).data


def test():
    workflow_name = "workflow_" + str(uuid.uuid4())

    wb = SimpleWhiteboard()
    with LzyRemoteEnv().workflow(name=workflow_name, whiteboard=wb):
        data = get_data(5 * 1024 * 1024 * 1024)
        wb.data = assert_big_data(len(data), data)
        wb_id = wb.__id__

    env = LzyRemoteEnv()
    wb_new = env.whiteboard(wb_id, SimpleWhiteboard)
    assert wb_new.data == data


if __name__ == "__main__":
    test()
