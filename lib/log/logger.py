"""Creates wrapper logger class."""
import logging
import os
from typing import Any, Dict

# if running in lambda, log to /tmp. Otherwise, log to the current directory.
if os.path.exists('/app'):
    log_directory = "/tmp"
else:
    log_directory = os.path.dirname(os.path.abspath(__file__))
if not os.path.exists(log_directory):
    os.makedirs(log_directory, exist_ok=True)

# log_filename = os.path.join(log_directory, f"logfile_{current_datetime_str}.log")  # noqa

# mypy: ignore-errors
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(filename)s]: %(message)s",
)

# mypy: ignore-errors


class Logger(logging.Logger):
    def __init__(self, name: str, level: int = logging.INFO) -> None:
        super().__init__(name, level)
        self.setLevel(level)

        # StreamHandler for printing to stdout
        stream_handler = logging.StreamHandler()
        stream_handler.setLevel(level)
        stream_handler.setFormatter(logging.Formatter(
            "%(asctime)s %(levelname)s [%(filename)s]: %(message)s"))

        # RotatingFileHandler for logging to a file
        # file_handler = RotatingFileHandler(
        #     log_filename, maxBytes=1024 * 1024, backupCount=5)
        # file_handler.setLevel(level)
        # file_handler.setFormatter(logging.Formatter(
        #     "%(asctime)s %(levelname)s [%(filename)s]: %(message)s"))

        self.addHandler(stream_handler)
        # self.addHandler(file_handler)

    def log(self, message: str, **kwargs: Dict[str, Any]) -> None:
        """Convenience method to log a message with some extra data."""
        self.info(message, extra=kwargs)


def get_logger(filename_dunder: str) -> Logger:
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
    ]
    root_idx = root_idx[0] if root_idx else 0
    joined_fp = '/'.join(word for word in split_fp[root_idx:])
    return Logger(name=joined_fp)
