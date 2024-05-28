"""Create Bluesky classes from dict.

Follows https://github.com/MarshalX/atproto/blob/main/packages/atproto_client/models/com/atproto/repo/strong_ref.py
"""  # noqa
from atproto_client.models.com.atproto.repo.strong_ref import Main as StrongRef


def create_strong_ref(strong_ref_dict: dict) -> StrongRef:
    return StrongRef(cid=strong_ref_dict["cid"], uri=strong_ref_dict["uri"])
