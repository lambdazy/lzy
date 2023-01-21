import re

NAME_VALID_SYMBOLS = "[alphanumeric characters,_,-]"


def is_name_valid(s: str) -> bool:
    return re.match("^[a-zA-Z0-9_-]*$", s) is not None
