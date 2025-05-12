import pytest
import pandas as pd
from unittest.mock import patch
from services.calculate_analytics.study_analytics.get_user_engagement import (
    get_agg_labels_for_engagements as agg,
)


class TestGetContentEngagedWith:
    """
    Unit tests for get_content_engaged_with.
    This function loads engagement records of a given type, filters by valid users, and returns a mapping from engaged post URI to a list of engagement dicts.
    """

    @patch(
        "services.calculate_analytics.study_analytics.get_user_engagement.get_agg_labels_for_engagements.load_data_from_local_storage"
    )
    def test_like_engagements(self, mock_load):
        """
        Test that likes are mapped correctly to URIs and users.
        Input: DataFrame with two likes by two users on two posts.
        Output: Dict mapping each post URI to a list of engagement dicts.
        """
        df = pd.DataFrame(
            [
                {
                    "uri": "uri_1",
                    "author": "did_A",
                    "synctimestamp": "2024-10-01",
                    "subject": '{"uri": "post_uri_1"}',
                },
                {
                    "uri": "uri_2",
                    "author": "did_B",
                    "synctimestamp": "2024-10-01",
                    "subject": '{"uri": "post_uri_2"}',
                },
            ]
        )
        expected_result = {
            "post_uri_1": [
                {"did": "did_A", "date": "2024-10-01", "record_type": "like"}
            ],
            "post_uri_2": [
                {"did": "did_B", "date": "2024-10-01", "record_type": "like"}
            ],
        }
        mock_load.return_value = df
        valid_dids = {"did_A", "did_B"}
        result = agg.get_content_engaged_with("like", valid_dids)
        assert set(result.keys()) == set(expected_result.keys())
        for uri, expected_engagements in expected_result.items():
            assert result[uri] == expected_engagements

    @patch(
        "services.calculate_analytics.study_analytics.get_user_engagement.get_agg_labels_for_engagements.load_data_from_local_storage"
    )
    def test_reply_engagements(self, mock_load):
        """
        Test that replies extract the parent URI correctly.
        Input: DataFrame with a reply record.
        Output: Dict mapping parent URI to engagement dict.
        """
        df = pd.DataFrame(
            [
                {
                    "uri": "reply_1",
                    "author": "did_C",
                    "synctimestamp": "2024-10-02",
                    "reply": '{"parent": {"uri": "post_uri_3"}}',
                },
            ]
        )
        expected_result = {
            "post_uri_3": [
                {"did": "did_C", "date": "2024-10-02", "record_type": "reply"}
            ]
        }
        mock_load.return_value = df
        valid_dids = {"did_C"}
        result = agg.get_content_engaged_with("reply", valid_dids)
        assert set(result.keys()) == set(expected_result.keys())
        for uri, expected_engagements in expected_result.items():
            assert result[uri] == expected_engagements


class TestGetEngagedContent:
    """
    Unit tests for get_engaged_content.
    This function aggregates all engagement types for valid users into a single URI-to-engagements mapping.
    """

    @patch(
        "services.calculate_analytics.study_analytics.get_user_engagement.get_agg_labels_for_engagements.get_content_engaged_with"
    )
    def test_aggregate_all_types(self, mock_get):
        """
        Test that all engagement types are merged into a single mapping.
        Input: Each engagement type returns a dict with one URI.
        Output: The merged dict contains all URIs and all engagement dicts.
        """
        mock_get.side_effect = [
            # likes
            {
                "uri_1": [
                    {"did": "did_A", "date": "2024-10-01", "record_type": "like"},
                    {"did": "did_B", "date": "2024-10-01", "record_type": "like"},
                    {"did": "did_C", "date": "2024-10-01", "record_type": "like"},
                ]
            },
            # posts
            {
                "uri_1": [
                    {"did": "did_A", "date": "2024-10-01", "record_type": "post"},
                ],
                "uri_2": [
                    {"did": "did_B", "date": "2024-10-01", "record_type": "post"},
                    {"did": "did_C", "date": "2024-10-01", "record_type": "post"},
                ],
            },
            # reposts
            {
                "uri_1": [
                    {"did": "did_B", "date": "2024-10-01", "record_type": "repost"},
                ],
                "uri_2": [
                    {"did": "did_D", "date": "2024-10-01", "record_type": "repost"},
                ],
                "uri_3": [
                    {"did": "did_C", "date": "2024-10-01", "record_type": "repost"},
                    {"did": "did_D", "date": "2024-10-01", "record_type": "repost"},
                ],
            },
            #
            {
                "uri_3": [
                    {"did": "did_C", "date": "2024-10-01", "record_type": "reply"},
                ],
                "uri_4": [
                    {"did": "did_D", "date": "2024-10-01", "record_type": "reply"},
                ],
            },
        ]
        expected_result = {
            "uri_1": [
                {"did": "did_A", "date": "2024-10-01", "record_type": "like"},
                {"did": "did_A", "date": "2024-10-01", "record_type": "post"},
                {"did": "did_B", "date": "2024-10-01", "record_type": "like"},
                {"did": "did_B", "date": "2024-10-01", "record_type": "repost"},
                {"did": "did_C", "date": "2024-10-01", "record_type": "like"},
            ],
            "uri_2": [
                {"did": "did_B", "date": "2024-10-01", "record_type": "post"},
                {"did": "did_C", "date": "2024-10-01", "record_type": "post"},
                {"did": "did_D", "date": "2024-10-01", "record_type": "repost"},
            ],
            "uri_3": [
                {"did": "did_C", "date": "2024-10-01", "record_type": "repost"},
                {"did": "did_C", "date": "2024-10-01", "record_type": "reply"},
                {"did": "did_D", "date": "2024-10-01", "record_type": "repost"},
            ],
            "uri_4": [
                {"did": "did_D", "date": "2024-10-01", "record_type": "reply"},
            ],
        }
        valid_dids = {"did_A", "did_B", "did_C", "did_D"}
        result = agg.get_engaged_content(valid_dids)
        assert set(result.keys()) == set(expected_result.keys())

        # check that the likes/reposts/posts/replies are all in the result
        # and are identical, in a clear and readable way, by organizing
        # by record type.
        for uri, expected_engagements in expected_result.items():
            observed_results_by_record_type = {}
            expected_results_by_record_type = {}
            for engagement in result[uri]:
                record_type = engagement["record_type"]
                if record_type not in observed_results_by_record_type:
                    observed_results_by_record_type[record_type] = []
                observed_results_by_record_type[record_type].append(engagement)
            for engagement in expected_engagements:
                record_type = engagement["record_type"]
                if record_type not in expected_results_by_record_type:
                    expected_results_by_record_type[record_type] = []
                expected_results_by_record_type[record_type].append(engagement)
            for (
                record_type,
                expected_engagements,
            ) in expected_results_by_record_type.items():
                assert (
                    observed_results_by_record_type[record_type] == expected_engagements
                )


class TestGetContentEngagedWithPerUser:
    """
    Unit tests for get_content_engaged_with_per_user.
    This function takes a URI-to-engagements mapping and returns a user/date/record_type mapping.
    """

    def test_per_user_mapping(self):
        """
        Test that engagements are grouped by user, date, and record type.
        Input: Two users, two dates, two record types.
        Output: Nested dict with correct structure and values.
        """
        engaged_content = {
            "uri_1": [
                {"did": "did_A", "date": "2024-10-01", "record_type": "like"},
                {"did": "did_B", "date": "2024-10-02", "record_type": "post"},
            ],
            "uri_2": [
                {"did": "did_A", "date": "2024-10-01", "record_type": "reply"},
            ],
        }
        result = agg.get_content_engaged_with_per_user(engaged_content)
        assert set(result.keys()) == {"did_A", "did_B"}
        assert set(result["did_A"].keys()) == {"2024-10-01"}
        assert set(result["did_B"].keys()) == {"2024-10-02"}
        assert set(result["did_A"]["2024-10-01"].keys()) == {
            "post",
            "like",
            "repost",
            "reply",
        }
        assert result["did_A"]["2024-10-01"]["like"][0]["post_uri"] == "uri_1"
        assert result["did_A"]["2024-10-01"]["reply"][0]["post_uri"] == "uri_2"
        assert result["did_B"]["2024-10-02"]["post"][0]["post_uri"] == "uri_1"


class TestGetLabelsForPartitionDate:
    """
    Unit tests for get_labels_for_partition_date.
    This function loads a DataFrame of labels for a given integration and partition date.
    """

    @patch(
        "services.calculate_analytics.study_analytics.get_user_engagement.get_agg_labels_for_engagements.load_data_from_local_storage"
    )
    def test_returns_deduped_df(self, mock_load):
        """
        Test that the function returns a DataFrame with duplicates dropped on 'uri'.
        Input: DataFrame with duplicate URIs.
        Output: DataFrame with only unique URIs.
        """
        df = pd.DataFrame(
            [
                {"uri": "uri_1", "prob_toxic": 0.1},
                {"uri": "uri_1", "prob_toxic": 0.2},
                {"uri": "uri_2", "prob_toxic": 0.3},
            ]
        )
        mock_load.return_value = df
        result = agg.get_labels_for_partition_date("perspective_api", "2024-10-01")
        assert set(result["uri"]) == {"uri_1", "uri_2"}
        assert len(result) == 2


class TestGetRelevantProbsForLabel:
    """
    Unit tests for get_relevant_probs_for_label.
    This function extracts relevant label probabilities for each integration type.
    """

    def test_perspective_api(self):
        """
        Test extraction for perspective_api integration.
        Input: Dict with prob_toxic and prob_constructive.
        Output: Dict with those keys and values.
        """
        label_dict = {"prob_toxic": 0.7, "prob_constructive": 0.2}
        result = agg.get_relevant_probs_for_label("perspective_api", label_dict)
        assert result == {"prob_toxic": 0.7, "prob_constructive": 0.2}

    def test_sociopolitical(self):
        """
        Test extraction for sociopolitical integration.
        Input: Dict with is_sociopolitical and political_ideology_label.
        Output: Dict with boolean flags for each ideology.
        """
        label_dict = {"is_sociopolitical": True, "political_ideology_label": "left"}
        result = agg.get_relevant_probs_for_label("sociopolitical", label_dict)
        assert result["is_sociopolitical"] is True
        assert result["is_not_sociopolitical"] is False
        assert result["is_political_left"] is True
        assert result["is_political_right"] is False
        assert result["is_political_moderate"] is False
        assert result["is_political_unclear"] is False

    def test_ime(self):
        """
        Test extraction for ime integration.
        Input: Dict with prob_intergroup, prob_moral, prob_emotion, prob_other.
        Output: Dict with those keys and values.
        """
        label_dict = {
            "prob_intergroup": 0.1,
            "prob_moral": 0.2,
            "prob_emotion": 0.3,
            "prob_other": 0.4,
        }
        result = agg.get_relevant_probs_for_label("ime", label_dict)
        assert result == label_dict

    def test_invalid_integration(self):
        """
        Test that an invalid integration raises ValueError.
        """
        with pytest.raises(ValueError):
            agg.get_relevant_probs_for_label("invalid", {})


class TestGetLabelsForEngagedContent:
    """
    Unit tests for get_labels_for_engaged_content.
    This function aggregates label probabilities for a list of URIs across integrations and partition dates.
    """

    @patch(
        "services.calculate_analytics.study_analytics.get_user_engagement.get_agg_labels_for_engagements.get_partition_dates"
    )
    @patch(
        "services.calculate_analytics.study_analytics.get_user_engagement.get_agg_labels_for_engagements.get_labels_for_partition_date"
    )
    @patch(
        "services.calculate_analytics.study_analytics.get_user_engagement.get_agg_labels_for_engagements.get_relevant_probs_for_label"
    )
    def test_labels_aggregation(self, mock_probs, mock_labels, mock_dates):
        """
        Test that the function aggregates label probabilities for each URI and integration.
        Input: Two URIs, two integrations, two partition dates, each with one label dict.
        Output: Dict mapping each URI to aggregated label probabilities.
        """
        uris = ["uri_1", "uri_2"]
        mock_dates.return_value = ["2024-10-01", "2024-10-02"]

        # Simulate two integrations, each with one label per URI per date
        def labels_side_effect(integration, partition_date):
            if integration == "perspective_api":
                return pd.DataFrame(
                    [
                        {"uri": "uri_1", "prob_toxic": 0.1},
                        {"uri": "uri_2", "prob_toxic": 0.2},
                    ]
                )
            elif integration == "sociopolitical":
                return pd.DataFrame(
                    [
                        {
                            "uri": "uri_1",
                            "is_sociopolitical": True,
                            "political_ideology_label": "left",
                        },
                        {
                            "uri": "uri_2",
                            "is_sociopolitical": False,
                            "political_ideology_label": "right",
                        },
                    ]
                )
            else:
                return pd.DataFrame([])

        mock_labels.side_effect = labels_side_effect

        def probs_side_effect(integration, label_dict):
            if integration == "perspective_api":
                return {"prob_toxic": label_dict["prob_toxic"]}
            elif integration == "sociopolitical":
                return {"is_sociopolitical": label_dict["is_sociopolitical"]}
            else:
                return {}

        mock_probs.side_effect = probs_side_effect
        result = agg.get_labels_for_engaged_content(uris)
        assert set(result.keys()) == {"uri_1", "uri_2"}
        assert "prob_toxic" in result["uri_1"]
        assert "is_sociopolitical" in result["uri_1"]
        assert result["uri_1"]["prob_toxic"] == 0.1
        assert result["uri_2"]["prob_toxic"] == 0.2
        assert result["uri_1"]["is_sociopolitical"] is True
        assert result["uri_2"]["is_sociopolitical"] is False


class TestGetColumnPrefixForRecordType:
    """
    Unit tests for get_column_prefix_for_record_type.
    This function returns the correct column prefix for each record type.
    """

    @pytest.mark.parametrize(
        "record_type,expected",
        [
            ("like", "prop_liked_posts"),
            ("post", "prop_posted_posts"),
            ("repost", "prop_reposted_posts"),
            ("reply", "prop_replied_posts"),
        ],
    )
    def test_valid_types(self, record_type, expected):
        """
        Test that valid record types return the correct prefix.
        """
        assert agg.get_column_prefix_for_record_type(record_type) == expected

    def test_invalid_type(self):
        """
        Test that an invalid record type raises ValueError.
        """
        with pytest.raises(ValueError):
            agg.get_column_prefix_for_record_type("invalid")


class TestGetPerUserPerDayContentLabelProportions:
    """
    Unit tests for get_per_user_per_day_content_label_proportions.
    This function links user engagement with content labels and computes per-user, per-day, per-record-type label proportions.
    """

    def test_basic_aggregation(self):
        """
        Test that the function computes correct label proportions for a simple case.
        Input: One user, one date, two record types, each with two URIs and binary labels.
        Output: Proportions for each label and record type.
        """
        user_to_content_engaged_with = {
            "did_A": {
                "2024-10-01": {
                    "like": ["uri_1", "uri_2"],
                    "post": ["uri_3"],
                    "repost": [],
                    "reply": [],
                }
            }
        }
        labels_for_engaged_content = {
            "uri_1": {
                "prob_toxic": 0.6,
                "prob_constructive": 0.4,
                "is_sociopolitical": 1,
            },
            "uri_2": {
                "prob_toxic": 0.2,
                "prob_constructive": 0.8,
                "is_sociopolitical": 0,
            },
            "uri_3": {
                "prob_toxic": 0.7,
                "prob_constructive": 0.9,
                "is_sociopolitical": 1,
            },
        }
        result = agg.get_per_user_per_day_content_label_proportions(
            user_to_content_engaged_with, labels_for_engaged_content
        )
        # Like: toxic > 0.5 for uri_1 only (1/2), constructive > 0.5 for uri_2 only (1/2), sociopolitical: (1+0)/2=0.5
        like = result["did_A"]["2024-10-01"]["prop_liked_posts_toxic"]
        assert like == 0.5
        like_constructive = result["did_A"]["2024-10-01"][
            "prop_liked_posts_constructive"
        ]
        assert like_constructive == 0.5
        like_socio = result["did_A"]["2024-10-01"]["prop_liked_posts_sociopolitical"]
        assert like_socio == 0.5
        # Post: toxic > 0.5 for uri_3 (1/1), constructive > 0.5 for uri_3 (1/1), sociopolitical: 1
        post = result["did_A"]["2024-10-01"]["prop_posted_posts_toxic"]
        assert post == 1.0
        post_constructive = result["did_A"]["2024-10-01"][
            "prop_posted_posts_constructive"
        ]
        assert post_constructive == 1.0
        post_socio = result["did_A"]["2024-10-01"]["prop_posted_posts_sociopolitical"]
        assert post_socio == 1.0
        # Repost and reply should be None
        assert result["did_A"]["2024-10-01"]["prop_reposted_posts_toxic"] is None
        assert result["did_A"]["2024-10-01"]["prop_replied_posts_toxic"] is None


class TestGetPerUserToWeeklyContentLabelProportions:
    """
    Unit tests for get_per_user_to_weekly_content_label_proportions.
    This function aggregates per-user, per-day label proportions into per-user, per-week averages.
    """

    def test_weekly_aggregation(self):
        """
        Test that the function averages daily proportions into weekly values, ignoring None.
        Input: One user, two days in the same week, each with a value for the same label.
        Output: Weekly average for that label.
        """
        user_per_day_content_label_proportions = {
            "did_A": {
                "2024-10-01": {"prop_liked_posts_toxic": 0.5},
                "2024-10-02": {"prop_liked_posts_toxic": 1.0},
            }
        }
        user_date_to_week_df = pd.DataFrame(
            {
                "bluesky_handle": ["did_A", "did_A"],
                "date": ["2024-10-01", "2024-10-02"],
                "week": ["week_1", "week_1"],
            }
        )
        result = agg.get_per_user_to_weekly_content_label_proportions(
            user_per_day_content_label_proportions, user_date_to_week_df
        )
        # Should average 0.5 and 1.0 to 0.75
        assert result["did_A"]["week_1"]["prop_liked_posts_toxic"] == 0.75

    def test_weekly_aggregation_with_none(self):
        """
        Test that None values are ignored in the weekly average, and all None yields None.
        """
        user_per_day_content_label_proportions = {
            "did_A": {
                "2024-10-01": {"prop_liked_posts_toxic": None},
                "2024-10-02": {"prop_liked_posts_toxic": 1.0},
            }
        }
        user_date_to_week_df = pd.DataFrame(
            {
                "bluesky_handle": ["did_A", "did_A"],
                "date": ["2024-10-01", "2024-10-02"],
                "week": ["week_1", "week_1"],
            }
        )
        result = agg.get_per_user_to_weekly_content_label_proportions(
            user_per_day_content_label_proportions, user_date_to_week_df
        )
        # Only one value (1.0), so average is 1.0
        assert result["did_A"]["week_1"]["prop_liked_posts_toxic"] == 1.0
        # All None yields None
        user_per_day_content_label_proportions = {
            "did_A": {
                "2024-10-01": {"prop_liked_posts_toxic": None},
                "2024-10-02": {"prop_liked_posts_toxic": None},
            }
        }
        result = agg.get_per_user_to_weekly_content_label_proportions(
            user_per_day_content_label_proportions, user_date_to_week_df
        )
        assert result["did_A"]["week_1"]["prop_liked_posts_toxic"] is None


class TestTransformPerUserToWeeklyContentLabelProportions:
    """
    Unit tests for transform_per_user_to_weekly_content_label_proportions.
    This function flattens the weekly label proportions into a DataFrame with user metadata.
    """

    def test_flattening(self):
        """
        Test that the function produces a DataFrame with the correct columns and values.
        Input: One user, one week, one label.
        Output: DataFrame with handle, condition, week, and label columns.
        """
        user_to_weekly_content_label_proportions = {
            "did_A": {"week_1": {"prop_liked_posts_toxic": 0.5}}
        }
        users = [
            {
                "bluesky_handle": "handle_A",
                "condition": "engagement",
                "bluesky_user_did": "did_A",
            }
        ]
        df = agg.transform_per_user_to_weekly_content_label_proportions(
            user_to_weekly_content_label_proportions, users
        )
        assert set(df.columns) >= {
            "handle",
            "condition",
            "week",
            "prop_liked_posts_toxic",
        }
        assert df.iloc[0]["handle"] == "handle_A"
        assert df.iloc[0]["condition"] == "engagement"
        assert df.iloc[0]["week"] == "week_1"
        assert df.iloc[0]["prop_liked_posts_toxic"] == 0.5


# --- Integration-style test data setup ---
USERS = [
    {"bluesky_user_did": f"did_{i}", "bluesky_handle": f"handle_{i}", "condition": cond}
    for i, cond in zip(
        range(1, 8),
        [
            "reverse_chronological",
            "engagement",
            "representative_diversification",
            "reverse_chronological",
            "engagement",
            "representative_diversification",
            "reverse_chronological",
        ],
    )
]
POSTS = [f"uri_{i}" for i in range(1, 10)]
WEEKS = {
    "2024-10-01": "week_1",
    "2024-10-02": "week_1",
    "2024-10-08": "week_2",
    "2024-10-15": "week_3",
}

# Engagements: user_id -> date -> {type: [uris]}
ENGAGEMENTS = {
    "did_1": {
        "2024-10-01": {
            "like": ["uri_1", "uri_2", "uri_3"],
            "post": ["uri_2", "uri_3"],
            "repost": [],
            "reply": [],
        },
        "2024-10-08": {
            "like": ["uri_4", "uri_5", "uri_6", "uri_9"],
            "post": [],
            "repost": ["uri_4", "uri_5", "uri_6"],
            "reply": [],
        },
    },
    "did_2": {
        "2024-10-08": {
            "like": ["uri_4", "uri_5", "uri_7", "uri_8"],
            "post": [],
            "repost": [],
            "reply": ["uri_1", "uri_2", "uri_9"],
        },
    },
    "did_3": {
        "2024-10-01": {
            "like": ["uri_2", "uri_3"],
            "post": ["uri_1"],
            "repost": [],
            "reply": [],
        },
        "2024-10-08": {
            "like": ["uri_4", "uri_7"],
            "post": [],
            "repost": ["uri_4", "uri_6", "uri_9"],
            "reply": ["uri_3", "uri_4", "uri_5"],
        },
    },
    "did_4": {
        "2024-10-08": {
            "like": ["uri_4", "uri_5", "uri_6", "uri_8", "uri_9"],
            "post": [],
            "repost": [],
            "reply": [],
        },
    },
    "did_5": {},  # No engagement
    "did_6": {
        "2024-10-01": {
            "like": ["uri_1", "uri_2"],
            "post": [],
            "repost": ["uri_3", "uri_4"],
            "reply": [],
        },
    },
    "did_7": {
        "2024-10-08": {"like": ["uri_8"], "post": [], "repost": [], "reply": []},
    },
}

# Labels: some posts missing labels
LABELS = {
    "uri_1": {"prob_toxic": 0.6, "prob_constructive": 0.4, "is_sociopolitical": 1},
    "uri_2": {"prob_toxic": 0.2, "prob_constructive": 0.8, "is_sociopolitical": 0},
    "uri_3": {"prob_toxic": 0.7, "prob_constructive": 0.9, "is_sociopolitical": 1},
    "uri_4": {"prob_toxic": 0.1, "prob_constructive": 0.2, "is_sociopolitical": 0},
    "uri_5": {"prob_toxic": 0.3, "prob_constructive": 0.7, "is_sociopolitical": 1},
    "uri_6": {"prob_toxic": 0.5, "prob_constructive": 0.5, "is_sociopolitical": 0},
    "uri_7": {"prob_toxic": 0.9, "prob_constructive": 0.1, "is_sociopolitical": 1},
    # uri_8, uri_9 missing labels
}

# --- Focused integration-style tests for each function ---


def test_get_content_engaged_with_per_user_complex():
    """
    Integration-style test for get_content_engaged_with_per_user with multiple users, posts, and engagement types.
    Ensures correct mapping of user/date/type to post URIs, including users with no engagement.
    """
    result = agg.get_content_engaged_with_per_user(
        {
            uri: [
                {"did": did, "date": date, "record_type": typ}
                for did, user_dates in ENGAGEMENTS.items()
                for date, types in user_dates.items()
                for typ, uris in types.items()
                if uri in uris
            ]
            for uri in POSTS
        }
    )
    # All users should be present
    assert set(result.keys()) == set(ENGAGEMENTS.keys())
    # User 5 should have no dates
    assert result["did_5"] == {}
    # User 1 should have correct dates and types
    assert "2024-10-01" in result["did_1"]
    assert "like" in result["did_1"]["2024-10-01"]
    assert any(x["post_uri"] == "uri_1" for x in result["did_1"]["2024-10-01"]["like"])


def test_get_labels_for_engaged_content_complex():
    """
    Integration-style test for get_labels_for_engaged_content with multiple users and posts.
    Ensures correct aggregation of label data, including missing labels.
    """
    uris = POSTS
    # Patch get_partition_dates and get_labels_for_partition_date to simulate label loading
    with (
        patch.object(agg, "get_partition_dates", return_value=["2024-10-01"]),
        patch.object(agg, "get_labels_for_partition_date") as mock_labels,
        patch.object(
            agg, "get_relevant_probs_for_label", side_effect=lambda integration, d: d
        ),
    ):

        def labels_side_effect(integration, partition_date):
            # Return a DataFrame with all available labels for the uris
            return pd.DataFrame(
                [{"uri": uri, **LABELS[uri]} for uri in uris if uri in LABELS]
            )

        mock_labels.side_effect = labels_side_effect
        result = agg.get_labels_for_engaged_content(uris)
        # All uris in LABELS should have label data
        for uri in LABELS:
            assert result[uri]["prob_toxic"] == LABELS[uri]["prob_toxic"]
        # Uris without labels should have empty dicts
        for uri in set(uris) - set(LABELS):
            assert result[uri] == {}


def test_get_per_user_per_day_content_label_proportions_complex():
    """
    Integration-style test for get_per_user_per_day_content_label_proportions with complex user/post/label data.
    Ensures correct computation of proportions, including None for missing labels and users with no engagement.
    """
    result = agg.get_per_user_per_day_content_label_proportions(ENGAGEMENTS, LABELS)
    # User 5 should have no days
    assert result["did_5"] == {}
    # User 1, 2024-10-01, like: should compute proportions for available labels
    like_props = result["did_1"]["2024-10-01"]["prop_liked_posts_toxic"]
    assert like_props is not None
    # User 4, 2024-10-08, like: some posts missing labels, so should handle None
    assert result["did_4"]["2024-10-08"]["prop_liked_posts_toxic"] is not None
    # User 7, only one like, missing label, should be None
    assert result["did_7"]["2024-10-08"]["prop_liked_posts_toxic"] is None


def test_get_per_user_to_weekly_content_label_proportions_complex():
    """
    Integration-style test for get_per_user_to_weekly_content_label_proportions with complex daily proportions.
    Ensures correct weekly aggregation, including None handling.
    """
    # Simulate user_date_to_week_df
    rows = []
    for did, user_dates in ENGAGEMENTS.items():
        for date in user_dates:
            rows.append(
                {"bluesky_handle": did, "date": date, "week": WEEKS.get(date, "week_1")}
            )
    user_date_to_week_df = pd.DataFrame(rows)
    # Use previous function to get daily proportions
    daily = agg.get_per_user_per_day_content_label_proportions(ENGAGEMENTS, LABELS)
    result = agg.get_per_user_to_weekly_content_label_proportions(
        daily, user_date_to_week_df
    )
    # User 5 should have week_1 with all None
    assert "week_1" in result["did_5"] or result["did_5"] == {}
    # User 1 should have week_1 and week_2
    assert set(result["did_1"].keys()) >= {"week_1", "week_2"}


def test_transform_per_user_to_weekly_content_label_proportions_complex():
    """
    Integration-style test for transform_per_user_to_weekly_content_label_proportions with complex weekly data.
    Ensures correct DataFrame output, all users present, and correct columns.
    """
    # Simulate weekly proportions
    rows = []
    for did in ENGAGEMENTS:
        rows.append({"week_1": {"prop_liked_posts_toxic": 0.5}})
    weekly = {did: {"week_1": {"prop_liked_posts_toxic": 0.5}} for did in ENGAGEMENTS}
    df = agg.transform_per_user_to_weekly_content_label_proportions(weekly, USERS)
    # All users should be present
    assert set(df["handle"]) == {u["bluesky_handle"] for u in USERS}
    assert "prop_liked_posts_toxic" in df.columns


def test_full_integration_flow():
    """
    Full integration test: run the complex scenario through all major functions in sequence.
    Ensures end-to-end correctness for all users, posts, and engagement types.
    """
    # Step 1: get_content_engaged_with_per_user
    per_user = agg.get_content_engaged_with_per_user(
        {
            uri: [
                {"did": did, "date": date, "record_type": typ}
                for did, user_dates in ENGAGEMENTS.items()
                for date, types in user_dates.items()
                for typ, uris in types.items()
                if uri in uris
            ]
            for uri in POSTS
        }
    )
    print(per_user)
    # Step 2: get_labels_for_engaged_content
    with (
        patch.object(agg, "get_partition_dates", return_value=["2024-10-01"]),
        patch.object(agg, "get_labels_for_partition_date") as mock_labels,
        patch.object(
            agg, "get_relevant_probs_for_label", side_effect=lambda integration, d: d
        ),
    ):

        def labels_side_effect(integration, partition_date):
            return pd.DataFrame(
                [{"uri": uri, **LABELS[uri]} for uri in POSTS if uri in LABELS]
            )

        mock_labels.side_effect = labels_side_effect
        labels = agg.get_labels_for_engaged_content(POSTS)
        print(labels)
    # Step 3: get_per_user_per_day_content_label_proportions
    daily = agg.get_per_user_per_day_content_label_proportions(ENGAGEMENTS, LABELS)
    # Step 4: get_per_user_to_weekly_content_label_proportions
    rows = []
    for did, user_dates in ENGAGEMENTS.items():
        for date in user_dates:
            rows.append(
                {"bluesky_handle": did, "date": date, "week": WEEKS.get(date, "week_1")}
            )
    user_date_to_week_df = pd.DataFrame(rows)
    weekly = agg.get_per_user_to_weekly_content_label_proportions(
        daily, user_date_to_week_df
    )
    # Step 5: transform_per_user_to_weekly_content_label_proportions
    df = agg.transform_per_user_to_weekly_content_label_proportions(weekly, USERS)
    # All users should be present
    assert set(df["handle"]) == {u["bluesky_handle"] for u in USERS}
    # User 5 should have all None for metrics
    assert df[df["handle"] == "handle_5"]["prop_liked_posts_toxic"].iloc[0] in (
        None,
        0.0,
    )
    # User 1 should have non-None for at least one metric
    assert df[df["handle"] == "handle_1"]["prop_liked_posts_toxic"].iloc[0] is not None
