"""Create Bluesky classes from dict.

Based on https://github.com/MarshalX/atproto/blob/main/packages/atproto_client/models/app/bsky/actor/defs.py
"""  # noqa
from atproto_client.models.app.bsky.actor.defs import ProfileViewBasic


def create_profileviewbasic(profile_view_basic_dict: dict) -> ProfileViewBasic:
    return ProfileViewBasic(
        did=profile_view_basic_dict["did"],
        handle=profile_view_basic_dict["handle"],
        associated=None,
        avatar=profile_view_basic_dict["avatar"],
        display_name=profile_view_basic_dict["display_name"],
        labels=None,
        viewer=None
    )
