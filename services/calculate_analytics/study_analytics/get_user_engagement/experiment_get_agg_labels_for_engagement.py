"""Testing logic for getting aggregated labels for content that's been
engaged with.

This is a custom integration test, not really a unit test, as I want to manually
QA each individual component (though could be easily turned into unit tests).

This is pretty tricky joining logic, so this helps make sure that the individual
components are working as expected.

Let's have the following scenario:
- There are 7 users (user_1 to user_7)
- There are 9 posts engaged with in some way (uri_1 to uri_9).
- This is over the course of 3 weeks.
    - Week 1: post 1, 2, 3
    - Week 2: post 4, 5, 6
    - Week 3: post 7, 8, 9
- Let's say that for the user to week map, they strictly follow a regular
Suday -> Sunday mapping.

Let's have the following engagements:
- User 1: Some likes and posts and reposts
    - Posts post 2
    - Posts post 3
    - Likes post 1
    - Likes post 2
    - Likes post 3
    - Likes post 4
    - Likes post 5
    - Likes post 6
    - Likes post 9
    - Reposted post 4
    - Reposted post 5
    - Reposted post 6
- User 2: Some replies + likes
    - Likes post 4
    - Likes post 5
    - Likes post 7
    - Likes post 8
    - Replies to post 1
    - Replies to post 2
    - Replies to post 9
- User 3: Some posts + likes + replies + reposts. Also liked+replied+reposted to the same post.
    - Posts post 1
    - Likes post 2
    - Likes post 3
    - Likes post 4
    - Likes post 7
    - Replies to post 3
    - Replies to post 4
    - Replies to post 5
    - Reposts post 4
    - Reposts post 6
    - Reposts post 9
- User 4: Some likes
    - Likes post 4
    - Likes post 5
    - Likes post 6
    - Likes post 8
    - Likes post 9
- User 5: None - inactive user.
- User 6: some reposts + likes
    - Likes post 1
    - Likes post 2
    - Reposts post 3
    - Reposts post 4
- User 7: only 1 like.
    - Likes post 8
"""

import json

import pandas as pd

users = [
    {
        "bluesky_user_did": "did_1",
        "bluesky_handle": "handle_1",
        "condition": "reverse_chronological",
    },
    {
        "bluesky_user_did": "did_2",
        "bluesky_handle": "handle_2",
        "condition": "engagement",
    },
    {
        "bluesky_user_did": "did_3",
        "bluesky_handle": "handle_3",
        "condition": "representative_diversification",
    },
    {
        "bluesky_user_did": "did_4",
        "bluesky_handle": "handle_4",
        "condition": "reverse_chronological",
    },
    {
        "bluesky_user_did": "did_5",
        "bluesky_handle": "handle_5",
        "condition": "engagement",
    },
    {
        "bluesky_user_did": "did_6",
        "bluesky_handle": "handle_6",
        "condition": "representative_diversification",
    },
    {
        "bluesky_user_did": "did_7",
        "bluesky_handle": "handle_7",
        "condition": "reverse_chronological",
    },
]

valid_study_users_dids = set([f"did_{i}" for i in range(1, 8)])

# TODO: use ths to stub 'get_content_engaged_with' when called with 'like'
like_records = [
    {
        "author": "<author>",
        "synctimestamp": "",
        "subject": json.dumps({"uri": "<post that was liked"}),
    }
]
post_records = []
reposted_records = []
replied_records = []

likes = pd.DataFrame(like_records)
posts = pd.DataFrame(post_records)
reposts = pd.DataFrame(reposted_records)
replies = pd.DataFrame(replied_records)

expected_map_uri_to_engagements = {
    "uri_1": [],
    "uri_2": [],
    "uri_3": [],
    "uri_4": [],
    "uri_5": [],
    "uri_6": [],
    "uri_7": [],
    "uri_8": [],
    "uri_9": [],
}

labels_for_engaged_content = {
    "uri_1": {},
    "uri_2": {},
    "uri_3": {},
    "uri_4": {},
    "uri_5": {},
    "uri_6": {},
    "uri_7": {},
    "uri_8": {},
    "uri_9": {},
}


def main():
    pass


if __name__ == "__main__":
    main()
