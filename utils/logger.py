import logging
import sys


def get_logger(is_verbose: bool) -> logging.Logger:
    """
    Возвращает логгер pacs_tcp_client.

    Args:
        is_verbose (bool): если True — уровень DEBUG, иначе INFO.

    Returns:
        logging.Logger: настроенный логгер без дублирования хэндлеров.
    """
    log_level = logging.DEBUG if is_verbose else logging.INFO
    logger = logging.getLogger("pacs_tcp_client")
    logger.setLevel(log_level)

    if not logger.handlers:  # предотвращает повторное добавление
        handler = logging.StreamHandler(sys.stdout)
        handler.setLevel(log_level)
        handler.setFormatter(
            logging.Formatter("[%(asctime)s] [%(levelname)s] - %(message)s")
        )
        logger.addHandler(handler)

    return logger