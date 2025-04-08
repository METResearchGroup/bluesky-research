from datetime import datetime

from lib.constants import timestamp_format

def timestamp_to_unix_microseconds(timestamp: str) -> int:
    """Convert a timestamp string to Unix microseconds.
    
    Args:
        timestamp: Timestamp string (in the codebase format, YYYY-MM-DD-HH:MM:SS)
    
    Returns:
        Unix microseconds since epoch
    """
    return int(datetime.strptime(timestamp, timestamp_format).timestamp() * 1_000_000)


def unix_microseconds_to_timestamp(unix_microseconds: int) -> str:
    """Convert Unix microseconds to a timestamp string.
    
    Args:
        unix_microseconds: Unix microseconds since epoch
    
    Returns:
        Timestamp string (in the codebase format, YYYY-MM-DD-HH:MM:SS)
    """
    return datetime.fromtimestamp(unix_microseconds / 1_000_000).strftime(timestamp_format)
