"""Creates wrapper logger class."""
import logging
from logging.handlers import RotatingFileHandler
import os
from typing import Any, Dict

log_directory = os.path.dirname(os.path.abspath(__file__))
if not os.path.exists(log_directory):
    os.makedirs(log_directory, exist_ok=True)

log_filename = os.path.join(log_directory, "logfile.log")

# mypy: ignore-errors
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(filename)s]: %(message)s",
    handlers=[
        logging.StreamHandler(),
        # Rotating log file, 1MB max size, keeping 5 backups
        RotatingFileHandler(log_filename, maxBytes=1024 * 1024, backupCount=5),
    ],
)


# mypy: ignore-errors
class Logger(logging.Logger):
    def __init__(self, name: str, level: int = logging.INFO) -> None:
        super().__init__(name, level)

    def log(self, message: str, **kwargs: Dict[str, Any]) -> None:
        """Convenience method to log a message with some extra data."""
        self.info(message, extra=kwargs)


def get_logger(filename_dunder: str) -> None:
    """Returns a logger with the appropriate filename.

    Filename defined by what is in the __file__ variable. Returns the path
    relative to the "bluesky-research" root directory.

    Expects the following usage:
    >>> logger = get_logger(__file__)
    """
    split_fp = filename_dunder.split('/')
    root_idx = [
        idx for idx, word in enumerate(split_fp)
        if word == "bluesky-research" or word == "bluesky_research"
    ][0]
    joined_fp = '/'.join(word for word in split_fp[root_idx:])
    return Logger(name=joined_fp)
