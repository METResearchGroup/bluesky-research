"""Creates wrapper logger class."""
import logging
from logging.handlers import RotatingFileHandler
import os
from typing import Any, Dict

log_directory = os.path.dirname(os.path.abspath(__file__))
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
