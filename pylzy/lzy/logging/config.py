import logging.config
import os
import string
from pathlib import Path
from typing import Optional

import yaml

LZY_LOGGING_CONFIG_PATH_ENV = "LZY_LOGGING_CONFIG_PATH"
LZY_LOGGING_LEVEL_ENV = "LZY_LOGGING_LEVEL"


def load_logging_config() -> None:
    level = os.environ.get(LZY_LOGGING_LEVEL_ENV, default="INFO")
    lzy_config_path: Optional[str] = os.getenv("LZY_LOGGING_CONFIG_ENV", default=None)
    path = Path(lzy_config_path) if lzy_config_path else Path(__file__).parent / "logging.yml"
    template = string.Template(path.read_text())
    config = yaml.safe_load(
        template.substitute(
            {
                LZY_LOGGING_LEVEL_ENV: level
            }
        )
    )
    logging.config.dictConfig(config)


def get_logger(name: str) -> logging.Logger:
    parent = logging.getLogger("lzy")
    if not name.startswith(parent.name + "."):
        logger = parent.getChild(name)
    else:
        logger = logging.getLogger(name)
    return logger
