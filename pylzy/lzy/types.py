from pathlib import Path


# we need to resolve concrete Path implementation
class File(type(Path())):  # type: ignore
    pass
