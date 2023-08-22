#!/usr/bin/env python

from lzy.api.v1 import Lzy, op, Provisioning, AutoPythonEnv


@Provisioning(cpu_count=1)
@op
def example1(i: int) -> int:
    return i + 1


if __name__ == '__main__':
    lzy = Lzy().with_provisioning(cpu_count=2).auth(
        user='vhaldemar',
        key_path='/home/lipkin/.ssh/private.pem',
        endpoint='localhost:8899',
    ).with_python_env(AutoPythonEnv(pypi_index_url='https://pypi.org/simple'))

    with lzy.workflow("example1").with_provisioning(ram_size_gb=1):
        result1 = example1(1)
        print(result1)
