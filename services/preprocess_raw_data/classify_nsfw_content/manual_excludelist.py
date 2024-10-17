"""Manual excludelist of users."""

import os

import pandas as pd

from transform.bluesky_helper import get_author_did_from_handle

NSFW_HANDLES = [
    "ruffusbleu.bsky.social",
    "fujiyamasamoyed.bsky.social",
    "peppermintdrake.bsky.social",
    "jackalcakes.bsky.social",
    "gunnerrott.bsky.social",
    "intenselycasual.bsky.social",
    "gnastystuff.bsky.social",
    "rai-brid.bsky.social",
    "xpray.bsky.social",
    "saphiros.hyper.wang",
    "tfiddlerart.bsky.social",
    "deriaz.bsky.social",
    "indiego.bsky.social",
    "soildweller.bsky.social",
    "ventiskull.bsky.social",
    "hortensjjja.bsky.social",
    "bonfiredemon.bsky.social",
    "mindofabear.bsky.social",
    "kekeflipnote.bsky.social",
    "simplespirits.bsky.social",
    "taggzzz.bsky.social",
    "purplebirdman.com",
    "effyneprin.bsky.social",
    "bunnyhazedayz.bsky.social",
    "nookdae.bsky.social",
    "orforf.bsky.social",
    "boosterpang.bsky.social",
    "blanclauz.bsky.social",
    "dksidediscovery.bsky.social",
    "doodledox.bsky.social",
    "thehelmetguy.bsky.social",
    "melonart.bsky.social",
    "dizdoodz.bsky.social",
    "evezara.bsky.social",
    "acgats.bsky.social",
    "chunie.bsky.social",
    "freeglassnsfw.bsky.social",
    "bigreddraken.bsky.social",
    "mahmapuu.bsky.social",
    "monokhrome.bsky.social",
    "dieselbrain.bsky.social",
    "nullghostart.bsky.social",
    "squigga.bsky.social",
    "barksquared.bsky.social",
    "jindw.bsky.social",
    "snao.bsky.social",
    "stow.bsky.social",
    "erotibot.bsky.social",
    "catsudon.art",
    "asumonokrom.bsky.social",
    "hemuchang.bsky.social",
    "tlagomars.bsky.social",
]

# art is nice, but users seem to prefer less art content
ART_HANDLES = [
    "oori.bsky.social",
    "zetrystan.bsky.social",
    "wolfskulljack.bsky.social",
    "b0tster.bsky.social",
    "kaatokunart.bsky.social",
    "xel44lex.bsky.social"
]

BSKY_HANDLES_TO_EXCLUDE = [
    "clarkrogers.bsky.social",
    "aleriiav.bsky.social",
    "momoru.bsky.social",
    "nanoless.bsky.social",
    "l4wless.bsky.social",
    "squeezable.bsky.social",
] + NSFW_HANDLES + ART_HANDLES


def export_csv(handles: list[str], dids_to_exclude: list[str]):
    dids = [
        {"did": did, "handle": handle} for did, handle in zip(dids_to_exclude, handles)
    ]
    df = pd.DataFrame(dids)
    df.to_csv("dids_to_exclude.csv", index=False)


def load_users_from_csv() -> pd.DataFrame:
    current_dir = os.path.dirname(os.path.abspath(__file__))
    users_to_exclude: pd.DataFrame = pd.read_csv(
        os.path.join(current_dir, "dids_to_exclude.csv")
    )
    return users_to_exclude

def load_users_to_exclude() -> dict[str, set]:
    users_to_exclude = load_users_from_csv()
    bsky_handles_to_exclude = set(users_to_exclude["handle"].tolist())
    bsky_dids_to_exclude = set(users_to_exclude["did"].tolist())
    return {
        "bsky_handles_to_exclude": bsky_handles_to_exclude,
        "bsky_dids_to_exclude": bsky_dids_to_exclude,
    }


def get_dids_to_exclude(load_existing_exclusions: bool = False) -> list[str]:
    if load_existing_exclusions:
        users_to_exclude: pd.DataFrame = load_users_from_csv()
    else:
        users_to_exclude = pd.DataFrame()
    if len(users_to_exclude) == 0:
        handle_to_did = {}
    else:
        handle_to_did = {
            row["handle"]: row["did"] for _, row in users_to_exclude.iterrows()
        }
    dids_to_exclude = []
    for handle in BSKY_HANDLES_TO_EXCLUDE:
        if handle in handle_to_did:
            did = handle_to_did[handle]
        else:
            did = get_author_did_from_handle(handle)
        dids_to_exclude.append(did)
    return dids_to_exclude

if __name__ == "__main__":
    dids_to_exclude = get_dids_to_exclude(load_existing_exclusions=True)
    export_csv(handles=BSKY_HANDLES_TO_EXCLUDE, dids_to_exclude=dids_to_exclude)
