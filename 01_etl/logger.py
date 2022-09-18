import logging
import datetime


handler = logging.FileHandler('etl.log')
logger = logging.getLogger('logger')
logger.setLevel(logging.INFO)
logger.addHandler(handler)


def log_to_file(message: str, _logger=logger) -> None:
    _logger.info(f"{datetime.datetime.now()} - "\
                 f"{message}")
