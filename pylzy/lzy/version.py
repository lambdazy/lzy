import os

__version__ = (
    open(os.path.dirname(os.path.abspath(__file__)) + "/version/version")
    .read()
    .rstrip()
)
