import logging
import sys


def get_logger(is_verbose):
    if is_verbose:
        log_level = logging.DEBUG
    else:
        log_level = logging.INFO

    log_format = logging.Formatter('[%(asctime)s] [%(levelname)s] - %(message)s')
    logger = logging.getLogger('pacs_tcp_client')
    logger.setLevel(log_level)

    # writing to stdout
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(log_level)
    handler.setFormatter(log_format)
    logger.addHandler(handler)

    return logger