"""While thumbing through the likes, I noticed a few handles that seemed
suspicious (e.g., only active during the course of the study, only a few
followers, no posts but only a few reposts, and thousands of likes for a
single user).

This script will help me QA these handles. Given the DID, I'll take a look
at the user's profile and see if they look spammy.
"""

import json
import os

from transform.bluesky_helper import get_author_record

current_dir = os.path.dirname(os.path.abspath(__file__))


dids = [
    "did:plc:2d35gojggpgcj3ghsdb5dhln",
    "did:plc:fc4ulii2crovr65svornykxp",
    "did:plc:43bcvyf6oyy33h2pdoltngt6",
    "did:plc:nlxnwxr4rwfibjc4wivilybg",
    "did:plc:f4gre6heruthkxbqu4zq23e5",
    "did:plc:xteqhfjaadv3nvnax6vb3tmq",
    "did:plc:amrcuoki4rergldcpvy767ha",
    "did:plc:3dg75jmphynsa2uiyxiiljoo",
    "did:plc:yxqbel77kopg5cmefmkk2la6",
    "did:plc:rxhxek5wj2sjxpe3q6mrjzix",
    "did:plc:rknptwjyvrzz3bupgw2es2fe",
    "did:plc:wodjaqy7dhnyqlsilwoc6tmj",
    "did:plc:7ugjibaavsumu5ccwte6ctbl",
    "did:plc:gnrivq4msxkv5d4h3jc2ro76",
    "did:plc:whmx5ov6p36yjrzrwdy7tyiv",
    "did:plc:lq2aacqbcuz5bfisqd4z4qjl",
    "did:plc:skhhpjyw2tcr5mzjexghu2yi",
    "did:plc:qzqtebctmkyae5nqq3owwyhw",
    "did:plc:m3ys5cyliq7nogyssqd3ihb6",
    "did:plc:ritystynfxtlaamtrl3nj6if",
    "did:plc:zyzy5w2l5yr5hbvsn4ql4sgr",
    "did:plc:dzncaojhiyjgegbbrysiobwh",
    "did:plc:etp37kxcyvaxekiog2ewwap3",
    "did:plc:cx4rygh5gsrzjcrsehzy33ra",
    "did:plc:zudxq4rc4tgpz4k3ddzuhrwb",
    "did:plc:qek4tzw4bca46pan2fwqxstw",
    "did:plc:ehhs5ococgvzt2u73dcm23t4",
    "did:plc:fhdazm35wpjtkt26n4dhulwq",
    "did:plc:k6spq3eia2wjaaj47sk442dv",
]


def get_link_for_did(did: str) -> str:
    """Get Bluesky profile link for the DID, so I can manually inspect it."""
    author_record = get_author_record(did=did)
    return f"https://bsky.app/profile/{author_record.handle}"


def main():
    dids_to_link = {}
    if os.path.exists(os.path.join(current_dir, "did_to_link.json")):
        with open(os.path.join(current_dir, "did_to_link.json"), "r") as f:
            dids_to_link = json.load(f)
    else:
        for did in dids:
            link = get_link_for_did(did)
            dids_to_link[did] = link
            with open(os.path.join(current_dir, "did_to_link.json"), "w") as f:
                json.dump(dids_to_link, f)


if __name__ == "__main__":
    main()
