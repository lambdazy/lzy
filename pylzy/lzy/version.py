import os

__version__ = (
    open(os.path.dirname(os.path.abspath(__file__)) + "/version/version")
    .read()
    .rstrip()
)

__homepage__ = 'https://lzy.ai'

__user_agent__ = f'pylzy {__version__} ({__homepage__})'
