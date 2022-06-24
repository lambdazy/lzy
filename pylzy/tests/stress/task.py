from time import sleep

from lzy.api.v1 import op
from lzy.api.v1.env import LzyRemoteEnv


class LzyTask:
    def __init__(self, func, func_name, args, kwargs):
        self.func = func
        self.func_name = func_name
        self.args = args
        self.kwargs = kwargs

    def __call__(self):
        @op
        def internal() -> None:
            self.func(*self.args, **self.kwargs)

        with LzyRemoteEnv().workflow(name=self.func_name):
            internal()


def durable_task(total_minutes, step_minutes):
    for i in range(total_minutes):
        sleep(60)
        if (i + 1) % step_minutes == 0:
            print(f"slept {(i + 1) // 60} hours {(i + 1) % 60} minutes")
    if total_minutes % step_minutes != 0:
        print(f"slept {total_minutes // 60} hours {total_minutes % 60} minutes")
