import logging.config
import random
from logging import Logger
import os
import string
from pathlib import Path
from typing import Optional, Dict, Any, cast

import yaml

LZY_LOG_CONFIG_PATH = "LZY_LOG_CONFIG_PATH"
LZY_LOG_LEVEL = "LZY_LOG_LEVEL"

LZY_SYSTEM_LOG_COLOR = "LZY_SYSTEM_LOG_COLOR"

# colours from https://github.com/tqdm/tqdm/blob/master/tqdm/std.py#L151
COLOURS = {'BLACK': '\x1b[30m', 'RED': '\x1b[31m', 'GREEN': '\x1b[32m',
           'YELLOW': '\x1b[33m', 'BLUE': '\x1b[34m', 'MAGENTA': '\x1b[35m',
           'CYAN': '\x1b[36m', 'WHITE': '\x1b[37m'}
RESET_COLOR = "\x1b[0m"


def get_color() -> str:
    color = os.environ.get(LZY_SYSTEM_LOG_COLOR, default="CYAN").upper()
    if color == "RANDOM":
        return list(COLOURS.keys())[random.randint(0, len(COLOURS) - 1)]
    if color not in COLOURS:
        raise ValueError(f'Color {color} is not supported. Supported colors are: {COLOURS.keys()}')
    return color


def get_logging_config() -> Dict[str, Any]:
    level = os.environ.get(LZY_LOG_LEVEL, default="INFO")
    color = get_color()

    lzy_config_path: Optional[str] = os.getenv("LZY_LOGGING_CONFIG_ENV", default=None)
    path = Path(lzy_config_path) if lzy_config_path else Path(__file__).parent / "logging.yml"
    template = string.Template(path.read_text())
    substitute = template.substitute({
        LZY_LOG_LEVEL: level,
        LZY_SYSTEM_LOG_COLOR: f"\\x1b{COLOURS[color][1:]}"
    })
    config = yaml.safe_load(substitute)
    return cast(Dict[str, Any], config)


def configure_logging(config: Optional[Dict[str, Any]] = None) -> None:
    if config is None:
        config = get_logging_config()
    logging.config.dictConfig(config)


def get_logger(name: str) -> Logger:
    parent = logging.getLogger("lzy")
    if not name.startswith(parent.name + "."):
        logger = parent.getChild(name)
    else:
        logger = logging.getLogger(name)
    return logger


def get_remote_logger(name: str) -> Logger:
    parent = logging.getLogger("remote")
    logger = parent.getChild(name)
    return logger
