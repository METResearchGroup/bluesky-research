"""Manual excludelist of users."""
import pandas as pd

from transform.bluesky_helper import get_author_did_from_handle

BSKY_HANDLES_TO_EXCLUDE = [
    "clarkrogers.bsky.social", "aleriiav.bsky.social",
]


def get_dids_to_exclude() -> list[str]:
    dids_to_exclude = []
    for handle in BSKY_HANDLES_TO_EXCLUDE:
        did = get_author_did_from_handle(handle)
        dids_to_exclude.append(did)
    return dids_to_exclude  


def export_csv(handles: list[str], dids_to_exclude: list[str]):
    dids = [{"did": did, "handle": handle} for did, handle in zip(dids_to_exclude, handles)]
    df = pd.DataFrame(dids)
    df.to_csv("dids_to_exclude.csv", index=False)


def load_users_to_exclude() -> list[str]:
    df = pd.read_csv("dids_to_exclude.csv")
    return df["did"].tolist()


if __name__ == "__main__":
    dids_to_exclude = get_dids_to_exclude()
    export_csv(handles=BSKY_HANDLES_TO_EXCLUDE, dids_to_exclude=dids_to_exclude)
