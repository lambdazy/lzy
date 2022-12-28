import logging
import sys

_log_format = f"[LZY LOCAL] - %(asctime)s - [%(levelname)s] - %(message)s"
_log_remote_format = f"%(asctime)s - [%(levelname)s] - %(message)s"


def __get_base_logger(name: str) -> logging.Logger:
    parent_logger = logging.getLogger("lzy")
    logger = parent_logger.getChild(name)
    logger.setLevel(logging.INFO)
    return logger


def get_logger(name: str) -> logging.Logger:
    logger = __get_base_logger(name)

    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setLevel(logging.INFO)
    stream_handler.setFormatter(logging.Formatter(_log_format))
    logger.addHandler(stream_handler)

    return logger


def get_remote_logger(name: str) -> logging.Logger:
    logger = __get_base_logger(name)

    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setLevel(logging.INFO)
    stream_handler.setFormatter(logging.Formatter(_log_remote_format))
    logger.addHandler(stream_handler)

    return logger
