import logging.config
from logging import Logger
import os
import string
from pathlib import Path
from typing import Optional, Dict, Any, cast

import yaml

LZY_LOG_CONFIG_PATH = "LZY_LOG_CONFIG_PATH"
LZY_LOG_LEVEL = "LZY_LOG_LEVEL"


def get_logging_config() -> Dict[str, Any]:
    level = os.environ.get(LZY_LOG_LEVEL, default="INFO")
    lzy_config_path: Optional[str] = os.getenv("LZY_LOGGING_CONFIG_ENV", default=None)
    path = Path(lzy_config_path) if lzy_config_path else Path(__file__).parent / "logging.yml"
    template = string.Template(path.read_text())
    config = yaml.safe_load(
        template.substitute(
            {
                LZY_LOG_LEVEL: level
            }
        )
    )
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
