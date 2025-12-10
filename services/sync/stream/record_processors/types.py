from typing import NamedTuple


class RecordRoute(NamedTuple):
    handler_key: str
    author_did: str
    filename: str
    metadata: dict
