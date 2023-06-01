import os
import subprocess
import sys
from typing import List

from lzy.api.v1 import op, Lzy

SUBPROCESS_ENV_VAR = 'SUBPROCESS'

# Some programs can be replicated by subprocesses, which will cause repeated execution of startup script. Here we test
# what such programs are completed successfully, in particular we check possibility of multiple input slots read and
# unicity of output slots write.


# copy-pasted from PyTorch Lightning multiprocessing
def _basic_subprocess_cmd() -> List[str]:
    import __main__  # local import to avoid https://github.com/Lightning-AI/lightning/issues/15218

    if __main__.__spec__ is None:  # pragma: no-cover
        return [sys.executable, os.path.abspath(sys.argv[0])] + sys.argv[1:]
    else:
        return [sys.executable, "-m", __main__.__spec__.name] + sys.argv[1:]


@op
def run(num: int) -> int:
    if os.getenv(SUBPROCESS_ENV_VAR):
        print('hello from subprocess')
        return 7

    print('hello from main process')

    env_copy = os.environ.copy()
    env_copy[SUBPROCESS_ENV_VAR] = '1'

    sub = subprocess.Popen(_basic_subprocess_cmd(), env=env_copy)
    sub_result = sub.wait()

    print(f'subprocess exit code: {sub_result}')

    return num * 2


def main():
    with Lzy().workflow(name='wf', interactive=False):
        result = run(21)
    print(f'main process result: {result}')


if __name__ == '__main__':
    main()
