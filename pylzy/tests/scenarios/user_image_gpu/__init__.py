import os
from typing import List

from lzy.api.v1 import op, Lzy, GpuType
import tensorflow as tf
from tensorflow.python.client import device_lib


def get_available_gpus() -> List[str]:
    local_device_protos = device_lib.list_local_devices()
    return [x.name for x in local_device_protos if x.device_type == 'GPU']


def is_inside_container() -> bool:
    return os.environ.get("LZY_INNER_CONTAINER") == "true"


@op(gpu_type=GpuType.V100.name, gpu_count=1)
def matrix_mult(a: List[List[float]], b: List[List[float]]) -> List[List[float]]:
    print(get_available_gpus())

    print("Executed inside container: " + str(is_inside_container()))

    m_a = tf.constant(a)
    m_b = tf.constant(b)
    m_c = tf.matmul(m_a, m_b)

    return m_c.numpy().tolist()


if __name__ == "__main__":
    lzy = Lzy()

    with lzy.workflow(name="wf", docker_image="lzydock/user-default:1.14.0", interactive=False):
        a = [[1.0, 2.0, 3.0], [4.0, 5.0, 6.0]]
        b = [[1.0, 2.0], [3.0, 4.0], [5.0, 6.0]]
        result = matrix_mult(a, b)
        print("Result: " + str(result))
