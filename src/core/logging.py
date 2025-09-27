import logging

from src.core.config import config


def get_logger(name: str):
    """
    Получить логгер
    :param name: название логгера
    :return: логгер
    """
    logger = logging.getLogger(name)
    if not logger.handlers:
        logger.setLevel(config.LOG_LEVEL)
        ch = logging.StreamHandler()
        ch.setLevel(config.LOG_LEVEL)
        formatter = logging.Formatter(
            "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
        )
        ch.setFormatter(formatter)
        logger.addHandler(ch)
    return logger
