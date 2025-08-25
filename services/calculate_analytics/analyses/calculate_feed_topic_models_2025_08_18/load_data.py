"""
This module contains the DataLoader class, which is used to load data from the local or production environment.
"""


class LocalDataLoader:
    pass


class ProdDataLoader:
    pass


class DataLoader:
    def __init__(self, mode: str):
        self.mode = mode

    def load_data(self):
        if self.mode == "local":
            return LocalDataLoader()
        elif self.mode == "prod":
            return ProdDataLoader()
        else:
            raise ValueError(f"Invalid mode: {self.mode}")
