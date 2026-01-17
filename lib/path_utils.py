import os


def create_directory_if_not_exists(path: str) -> None:
    """Creates a directory if it does not exist."""
    dirpath = os.path.dirname(path)
    if not os.path.exists(dirpath):
        os.makedirs(dirpath)
