import subprocess
import sys
from lzy.api import op, LzyEnv


def install():
    subprocess.check_call([sys.executable, "setup.py", "install"], cwd="some_local_module")


@op
def main() -> int:
    from some_local_module.src import SOME_VAL
    return 0


if __name__ == "__main__":
    install()
    with LzyEnv(user='phil'):
        main()
