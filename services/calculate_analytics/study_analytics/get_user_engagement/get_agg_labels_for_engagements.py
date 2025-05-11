"""Get content classifications for engaged content, and summarizes
the per-user, per-week aggregate averages.

Writes a pandas df where the columns are a pairwise combination of:

- Engagement type: post, like, repost, reshare
- Content class:
    - Perspective API: toxic, constructive
    - Sociopolitical: political, not political, left, moderate, right, unclear
    - IME: intergroup, moral, emotion, other

For example, for toxic, we'll have the following columns
- prop_liked_posts_toxic
- prop_posted_posts_toxic
- prop_reshared_posts_toxic
- prop_reposted_posts_toxic

Creates 4 * 12 = 48 columns related to content classification.

In addition, we have a few more columns:
- Bluesky Handle
- Week
- Condition

We export this as a .csv file.

# TODO: should look into refactoring at some point with the feed averages logic,
as this'll mostly copy that anyways. But would be good to refactor for future
purposes.

First, we need to get the posts that users engaged with.
    - For a partition date:
        # load the user engagement records
        - Create a local hash map, for the given date and across all record types,
        of URI to user handle. This'll help with the lookup, and each data type
        will have a type-specific URI anyways.
            - Something like:
                {
                    "<uri>": [
                        {
                            "handle": "<handle_1>",
                            "timestamp": "<YYYY-MM-DD engaged with>",
                            "record_type": "<record_type>"
                        },
                        ...
                        {
                            "handle": "<handle_2>",
                            "timestamp": "<YYYY-MM-DD engaged with>",
                            "record_type": "<record_type>"
                        },
                        ...
                    ]
                }
            - This needs to be the URI of the post itself, not just the record.
            For example, for a like, it shouldn't be the like URI, but rather
            the URI of the post that was liked.
            - This needs to be a list of dicts, since multiple users can like
            the same post, for example.
            - It can be possible that the same post URI is referenced in different
            ways, i.e., for a given post, user A can like it and user B can repost
            it, so we want to account for these accordingly.
            - We need to record when each user engaged with the post, so that
            we can timestamp their activity correctly.

        - Create a map of handle to engagements per day.
            - Something like:
                {
                    "handle": {
                        "<date>": {
                            "post": [],
                            "like": [],
                            "repost": [],
                            "reshare": []
                        }
                    }
                }

        - For each record type:
            - Load all posts/likes/reposts/reshares from `raw_sync`
            - Filter for those whose author is in the study.
            - Update the local hash map accordingly.
                - Again, URI of the post should be used as the key.

        At this point, we'll have a local hash map of all the post URIs that have
        corresponding engagements (e.g., all the posts that were liked on
        2024-10-01).

        Then, we need to get the posts liked/reposted/shared. We can have a default
        lookback logic (e.g,. 7 days) to try to get the base post that was
        liked/reposted/reshared.

        Export these, something like 'posts_engaged_with', with just the URI
        and timestamp, partitioned by 'partition_date'.

        This gives, for each partition date, the posts written in those days
        that were then interacted with at some point.

After having the 'posts_engaged_with', we now hydrate those with the labels.
    - Create a hash map for posts to labels:
        - Something like:
            {
                "<uri>": {
                    "prob_toxic",
                    "prob_constructive",
                    ...
                }
            }

        I should be able to just load this in memory across all
    - For each partition date.
        - Load all the posts engaged with
        - Load all the labels
            - For each integration:
                - Load all the records for that day.
                - Filter for the URIs that exist in the hash map of URIs
                engaged with.

    - At the end, we should have a global hash map of all posts engaged with,
    whose key = URI and whose values are the labels.

Then, we loop through the Bluesky handles and start calculating the averages.
    - The format that we start with is:
        {
            "handle": {
                "<date>": {
                    "post": [],
                    "like": [],
                    "repost": [],
                    "reshare": []
                }
            }
        }
    - We loop through all of these.
        - For handle in handles:
            For date in dates:
                For record_type in ["post", "like", "repost", "reshare"]:
                    - Do a lookup of URI to hash map, to get the labels:
                        {
                            "<uri>": {
                                "prob_toxic",
                                "prob_constructive",
                                ...
                            }
                        }
                    - Calculate the proportion of each label per date.

                - For a given date, the output should be something like:
                    {
                        "<date>": {
                            "prop_liked_posts_toxic",
                            "prop_posted_posts_toxic",
                            "prop_reshared_posts_toxic",
                            "prop_reposted_posts_toxic",
                            ...
                            "prop_liked_posts_is_political",
                            "prop_posted_posts_is_political",
                            "prop_reshared_posts_is_political",
                            "prop_reposted_posts_is_political"
                        }
                    }

                - If there are no posts, i.e., for handle X on 2024-10-01, they
                didn't have any shared posts, we default to None. We don't want
                to default to 0 since a p=0 has an actual meaning.

    - We now have something of the shape:
        {
            "<handle>": {
                "<date>": {
                    "prop_liked_posts_toxic",
                    "prop_posted_posts_toxic",
                    "prop_reshared_posts_toxic",
                    "prop_reposted_posts_toxic",
                    ...
                    "prop_liked_posts_is_political",
                    "prop_posted_posts_is_political",
                    "prop_reshared_posts_is_political",
                    "prop_reposted_posts_is_political"
                }
            }
        }

    - Now we start aggregating per week:
        - We load the hash map of user, day, and week.
            - subset_user_date_to_week_df = user_date_to_week_df[
                user_date_to_week_df["bluesky_handle"] == user
            ]
            -  subset_map_date_to_week = dict(
                    zip(
                        subset_user_date_to_week_df["date"],
                        subset_user_date_to_week_df["week"],
                    )
                )
        - We create a hash map of the following format, where we append
        the proportions across each day.
            {
                "<handle>": {
                    "week_1": {
                        "prop_liked_posts_toxic": [],
                        "prop_posted_posts_toxic": [],
                        "prop_reshared_posts_toxic": [],
                        "prop_reposted_posts_toxic": [],
                        ...
                        "prop_liked_posts_is_political": [],
                        "prop_posted_posts_is_political": [],
                        "prop_reshared_posts_is_political": [],
                        "prop_reposted_posts_is_political": []
                    },
                    ...
                }
            }
        - For each partition date:
            - We get the corresponding week.
            - We append the proportions across each day to the hash map.

        - We now have our hydrated hash map. We then iterate through each week
        and find the average for that week
            - We exclude any None/NaNs. If the resulting array is empty,
            we return Nan.

            - The output can be something like this:
                {
                    "<handle>": {
                        "week_1": {
                            "prop_liked_posts_toxic": 0.0001,
                            "prop_posted_posts_toxic": None,
                            "prop_reshared_posts_toxic": None,
                            "prop_reposted_posts_toxic": 0.0005,
                            ...
                            "prop_liked_posts_is_political": 0.002,
                            "prop_posted_posts_is_political": None,
                            "prop_reshared_posts_is_political": None,
                            "prop_reposted_posts_is_political": None
                        },
                        ...
                    }
                }

Now, we flatten.
    - Take each combination of handle + week and make that a separate row.
    - Input:
        {
            "<handle>": {
                "week_1": {
                    "prop_liked_posts_toxic": 0.0001,
                    "prop_posted_posts_toxic": None,
                    "prop_reshared_posts_toxic": None,
                    "prop_reposted_posts_toxic": 0.0005,
                    ...
                    "prop_liked_posts_is_political": 0.002,
                    "prop_posted_posts_is_political": None,
                    "prop_reshared_posts_is_political": None,
                    "prop_reposted_posts_is_political": None
                },
                "week_2": {
                    "prop_liked_posts_toxic": 0.0001,
                    "prop_posted_posts_toxic": None,
                    "prop_reshared_posts_toxic": None,
                    "prop_reposted_posts_toxic": 0.0005,
                    ...
                    "prop_liked_posts_is_political": 0.002,
                    "prop_posted_posts_is_political": None,
                    "prop_reshared_posts_is_political": None,
                    "prop_reposted_posts_is_political": None
                },
                ...
            }
        }
    - Output:
        [
            {
                "handle": "<handle>",
                "week": 1,
                "prop_liked_posts_toxic": 0.0001,
                "prop_posted_posts_toxic": None,
                "prop_reshared_posts_toxic": None,
                "prop_reposted_posts_toxic": 0.0005,
                ...
                "prop_liked_posts_is_political": 0.002,
                "prop_posted_posts_is_political": None,
                "prop_reshared_posts_is_political": None,
                "prop_reposted_posts_is_political": None
            },
            {
                "handle": "<handle>",
                "week": 2,
                "prop_liked_posts_toxic": 0.0001,
                "prop_posted_posts_toxic": None,
                "prop_reshared_posts_toxic": None,
                "prop_reposted_posts_toxic": 0.0005,
                ...
                "prop_liked_posts_is_political": 0.002,
                "prop_posted_posts_is_political": None,
                "prop_reshared_posts_is_political": None,
                "prop_reposted_posts_is_political": None
            }
        ]

    - Then we convert this to a pandas dataframe and export.

"""

import os

import pandas as pd

current_dir = os.path.dirname(os.path.abspath(__file__))
filename = "content_classifications_engaged_content_per_user_per_week.csv"
fp = os.path.join(current_dir, filename)


def get_engaged_content():
    pass


def get_agg_labels_content_per_user_per_day():
    pass


def get_agg_labels_content_per_user_per_week():
    pass


def transform_labeled_content_per_user_per_week():
    pass


def main():
    transformed_agg_labeled_engaged_content_per_user_per_week: pd.DataFrame = (
        pd.DataFrame()
    )
    transformed_agg_labeled_engaged_content_per_user_per_week.to_csv(fp, index=False)


if __name__ == "__main__":
    main()
