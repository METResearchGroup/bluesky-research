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
    "terribleanimal.com",
    "poch4n.bsky.social",
    "waspsaladsfw.bsky.social",
    "aruurara.bsky.social",
    "skyaboveme.bsky.social",
    "onlineworms.bsky.social",
    "bacun.bsky.social",
    "zakimpo.bsky.social",
    "sammytighe.bsky.social",
]

# art is nice, but users seem to prefer less art content
ART_HANDLES = [
    "oori.bsky.social",
    "zetrystan.bsky.social",
    "wolfskulljack.bsky.social",
    "b0tster.bsky.social",
    "kaatokunart.bsky.social",
    "xel44lex.bsky.social",
    "rosuuri.bsky.social",
    "maxoke.bsky.social",
    "some1else45.bsky.social",
    "toorurii.bsky.social",
    "cillia.bsky.social",
    "t1kosewad.bsky.social",
    "sh0daigoji.bsky.social",
    "dasdokter.bsky.social",
    "nonoworks.bsky.social",
    "picklez.bsky.social",
    "hcnone.bsky.social",
    "touyarokii.bsky.social",
    "toranagakiryu.bsky.social",
    "namie.bsky.social",
    "velinxi.bsky.social",
    "kyutsii.bsky.social",
    "e7lilyy.bsky.social",
    "mayexplode.bsky.social",
    "mogu-sq.bsky.social",
    "wooperfuri.bsky.social",
    "princesshinghoi.bsky.social",
    "keiseeaaa.bsky.social",
    "mint-tan.bsky.social",
    "keiblegh.bsky.social",
    "kianamai.bsky.social",
    "maruccy.bsky.social",
    "comfythighs.bsky.social",
    "alchemaniac.bsky.social",
    "distr.bsky.social",
    "kurohshiro.bsky.social",
    "me1mezu.bsky.social",
    "missfaves.com",
    "daaku.bsky.social",
    "darkavey.bsky.social",
    "poisonp1nk.bsky.social",
    "octaviusdp.bsky.social",
    "e7lilyy.bsky.social",
    "roikopi.bsky.social",
    "potassium.bsky.social",
    "rinstinks.bsky.social",
    "overmikii.bsky.social",
    "ezdrools.bsky.social",
    "vexoriathesuneater.bsky.social",
    "ninochuu.bsky.social",
    "bootoober.bsky.social",
    "puppyypawss.bsky.social",
    "clickdraws.bsky.social",
    "namikare4.bsky.social",
    "paripariarts.bsky.social",
    "websake.bsky.social",
    "nycnouu.bsky.social",
    "eu03.bsky.social",
    "sherribon.bsky.social",
    "arainydancer.bsky.social",
    "theycallhimcake.bsky.social",
    "magui3.bsky.social",
    "afterfield.bsky.social",
    "arlecchumi.bsky.social",
    "vyragami.bsky.social",
    "magui3.bsky.social",
    "mikeluckas.bsky.social",
    "limealicious.bsky.social",
    "grimmivt.bsky.social",
    "zaphnart.bsky.social",
    "megalodonvt.bsky.social",
    "vyragami.bsky.social",
    "thijikoy.bsky.social",
    "ggelus.bsky.social",
    "mittsumia.bsky.social",
    "cinderone.bsky.social",
    "beneceadraws.bsky.social",
    "meyoco.bsky.social",
    "sydusarts.bsky.social",
    "monstersovka.bsky.social",
    "limart05.bsky.social",
    "maddestmao.com",
    "par0llel.bsky.social",
    "kurisuwu.bsky.social",
    "merryweatherey.bsky.social",
    "stellizard.bsky.social",
    "inimeitiel.bsky.social",
]

BSKY_HANDLES_TO_EXCLUDE = (
    [
        "clarkrogers.bsky.social",
        "aleriiav.bsky.social",
        "momoru.bsky.social",
        "nanoless.bsky.social",
        "l4wless.bsky.social",
        "squeezable.bsky.social",
    ]
    + NSFW_HANDLES
    + ART_HANDLES
)


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
    bsky_handles_to_exclude: set[str] = set(users_to_exclude["handle"].tolist())
    bsky_dids_to_exclude: set[str] = set(users_to_exclude["did"].tolist())
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
