"""Manual excludelist of users."""

import os

import pandas as pd

from transform.bluesky_helper import get_author_did_from_handle

FURRIES_HANDLES = [
    "ruffusbleu.bsky.social",
    "fujiyamasamoyed.bsky.social",
    "peppermintdrake.bsky.social",
]

BSKY_HANDLES_TO_EXCLUDE = [
    "clarkrogers.bsky.social",
    "aleriiav.bsky.social",
    "momoru.bsky.social",
    "nanoless.bsky.social",
    "l4wless.bsky.social",
    "squeezable.bsky.social",
] + FURRIES_HANDLES


def get_dids_to_exclude() -> list[str]:
    dids_to_exclude = []
    for handle in BSKY_HANDLES_TO_EXCLUDE:
        did = get_author_did_from_handle(handle)
        dids_to_exclude.append(did)
    return dids_to_exclude


def export_csv(handles: list[str], dids_to_exclude: list[str]):
    dids = [
        {"did": did, "handle": handle} for did, handle in zip(dids_to_exclude, handles)
    ]
    df = pd.DataFrame(dids)
    df.to_csv("dids_to_exclude.csv", index=False)


def load_users_to_exclude() -> dict[str, set]:
    current_dir = os.path.dirname(os.path.abspath(__file__))
    users_to_exclude: pd.DataFrame = pd.read_csv(
        os.path.join(current_dir, "dids_to_exclude.csv")
    )
    bsky_handles_to_exclude = set(users_to_exclude["handle"].tolist())
    bsky_dids_to_exclude = set(users_to_exclude["did"].tolist())
    return {
        "bsky_handles_to_exclude": bsky_handles_to_exclude,
        "bsky_dids_to_exclude": bsky_dids_to_exclude,
    }


if __name__ == "__main__":
    dids_to_exclude = get_dids_to_exclude()
    export_csv(handles=BSKY_HANDLES_TO_EXCLUDE, dids_to_exclude=dids_to_exclude)
