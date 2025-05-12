import pytest
import pandas as pd
from unittest.mock import patch
from services.calculate_analytics.study_analytics.get_user_engagement import (
    get_agg_labels_for_engagements as agg,
)
from services.calculate_analytics.study_analytics.get_user_engagement.constants import (
    default_content_engagement_columns,
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
    def test_post_engagements(self, mock_load):
        """
        Test that posts use their own URI as the engaged post URI.
        Input: DataFrame with post records.
        Output: Dict mapping post URI to engagement dict.
        """
        df = pd.DataFrame(
            [
                {
                    "uri": "post_uri_1",
                    "author": "did_A",
                    "synctimestamp": "2024-10-01",
                },
                {
                    "uri": "post_uri_2",
                    "author": "did_B",
                    "synctimestamp": "2024-10-02",
                },
            ]
        )
        expected_result = {
            "post_uri_1": [
                {"did": "did_A", "date": "2024-10-01", "record_type": "post"}
            ],
            "post_uri_2": [
                {"did": "did_B", "date": "2024-10-02", "record_type": "post"}
            ],
        }
        mock_load.return_value = df
        valid_dids = {"did_A", "did_B"}
        result = agg.get_content_engaged_with("post", valid_dids)
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
                {"did": "did_C", "date": "2024-10-02", "record_type": "like"},
            ],
            "uri_2": [
                {"did": "did_A", "date": "2024-10-01", "record_type": "like"},
                {"did": "did_A", "date": "2024-10-01", "record_type": "reply"},
            ],
            "uri_3": [
                {"did": "did_A", "date": "2024-10-01", "record_type": "post"},
                {"did": "did_B", "date": "2024-10-02", "record_type": "like"},
                {"did": "did_C", "date": "2024-10-02", "record_type": "repost"},
            ],
            "uri_4": [
                {"did": "did_A", "date": "2024-10-01", "record_type": "reply"},
                {"did": "did_B", "date": "2024-10-03", "record_type": "reply"},
                {"did": "did_C", "date": "2024-10-03", "record_type": "like"},
                {"did": "did_C", "date": "2024-10-05", "record_type": "repost"},
                {"did": "did_D", "date": "2024-10-05", "record_type": "like"},
            ],
            "uri_5": [
                {"did": "did_A", "date": "2024-10-01", "record_type": "post"},
                {"did": "did_B", "date": "2024-10-02", "record_type": "like"},
                {"did": "did_C", "date": "2024-10-03", "record_type": "like"},
                {"did": "did_D", "date": "2024-10-03", "record_type": "like"},
            ],
        }
        expected_result = {
            "did_A": {
                "2024-10-01": {
                    "like": ["uri_1", "uri_2"],
                    "post": ["uri_3", "uri_5"],
                    "reply": ["uri_2", "uri_4"],
                    "repost": [],
                },
            },
            "did_B": {
                "2024-10-02": {
                    "post": ["uri_1"],
                    "like": ["uri_3", "uri_5"],
                    "repost": [],
                    "reply": [],
                },
                "2024-10-03": {
                    "like": [],
                    "post": [],
                    "repost": [],
                    "reply": ["uri_4"],
                },
            },
            "did_C": {
                "2024-10-02": {
                    "like": ["uri_1"],
                    "post": [],
                    "repost": ["uri_3"],
                    "reply": [],
                },
                "2024-10-03": {
                    "like": ["uri_4", "uri_5"],
                    "post": [],
                    "repost": [],
                    "reply": [],
                },
                "2024-10-05": {
                    "like": [],
                    "post": [],
                    "repost": ["uri_4"],
                    "reply": [],
                },
            },
            "did_D": {
                "2024-10-03": {
                    "like": ["uri_5"],
                    "post": [],
                    "repost": [],
                    "reply": [],
                },
                "2024-10-05": {
                    "like": ["uri_4"],
                    "post": [],
                    "repost": [],
                    "reply": [],
                },
            },
        }
        result = agg.get_content_engaged_with_per_user(engaged_content)
        assert set(result.keys()) == set(expected_result.keys())

        # Check that the structure and content match the expected result
        for did, expected_dates in expected_result.items():
            assert set(result[did].keys()) == set(expected_dates.keys())

            for date, expected_record_types in expected_dates.items():
                assert set(result[did][date].keys()) == set(
                    expected_record_types.keys()
                )

                for record_type, expected_uris in expected_record_types.items():
                    observed_uris = result[did][date][record_type]
                    assert len(observed_uris) == len(expected_uris)

                    # Sort both lists to ensure consistent comparison
                    observed_sorted = sorted(observed_uris)
                    expected_sorted = sorted(expected_uris)

                    assert observed_sorted == expected_sorted


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
        Input: Two URIs, three integrations (perspective_api, sociopolitical, ime), two partition dates, each with one label dict.
        Output: Dict mapping each URI to aggregated label probabilities.
        """
        uris = ["uri_1", "uri_2"]
        mock_dates.return_value = ["2024-10-01", "2024-10-02"]

        # Simulate three integrations, each with one label per URI per date
        def labels_side_effect(integration, partition_date):
            if integration == "perspective_api":
                return pd.DataFrame(
                    [
                        {"uri": "uri_1", "prob_toxic": 0.7, "prob_constructive": 0.2},
                        {"uri": "uri_2", "prob_toxic": 0.3, "prob_constructive": 0.8},
                        {
                            "uri": "uri_3",
                            "prob_toxic": 0.5,
                            "prob_constructive": 0.5,
                        },  # shouldn't show up.
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
                        # shouldn't show up.
                        {
                            "uri": "uri_3",
                            "is_sociopolitical": True,
                            "political_ideology_label": "moderate",
                        },
                    ]
                )
            elif integration == "ime":
                return pd.DataFrame(
                    [
                        {
                            "uri": "uri_1",
                            "prob_intergroup": 0.4,
                            "prob_moral": 0.3,
                            "prob_emotion": 0.2,
                            "prob_other": 0.1,
                        },
                        {
                            "uri": "uri_2",
                            "prob_intergroup": 0.1,
                            "prob_moral": 0.2,
                            "prob_emotion": 0.3,
                            "prob_other": 0.4,
                        },
                        # shouldn't show up.
                        {
                            "uri": "uri_3",
                            "prob_intergroup": 0.5,
                            "prob_moral": 0.5,
                            "prob_emotion": 0.5,
                            "prob_other": 0.5,
                        },
                    ]
                )
            else:
                return pd.DataFrame([])

        mock_labels.side_effect = labels_side_effect

        def probs_side_effect(integration, label_dict):
            if integration == "perspective_api":
                return {
                    "prob_toxic": label_dict["prob_toxic"],
                    "prob_constructive": label_dict["prob_constructive"],
                }
            elif integration == "sociopolitical":
                return {
                    "is_sociopolitical": label_dict["is_sociopolitical"],
                    "is_not_sociopolitical": not label_dict["is_sociopolitical"],
                    "is_political_left": (
                        label_dict["political_ideology_label"] == "left"
                    ),
                    "is_political_right": (
                        label_dict["political_ideology_label"] == "right"
                    ),
                    "is_political_moderate": (
                        label_dict["political_ideology_label"] == "moderate"
                    ),
                    "is_political_unclear": (
                        label_dict["political_ideology_label"] == "unclear"
                    ),
                }
            elif integration == "ime":
                return {
                    "prob_intergroup": label_dict["prob_intergroup"],
                    "prob_moral": label_dict["prob_moral"],
                    "prob_emotion": label_dict["prob_emotion"],
                    "prob_other": label_dict["prob_other"],
                }
            else:
                return {}

        mock_probs.side_effect = probs_side_effect

        expected_result = {
            "uri_1": {
                "prob_toxic": 0.7,
                "prob_constructive": 0.2,
                "is_sociopolitical": True,
                "is_not_sociopolitical": False,
                "is_political_left": True,
                "is_political_right": False,
                "is_political_moderate": False,
                "is_political_unclear": False,
                "prob_intergroup": 0.4,
                "prob_moral": 0.3,
                "prob_emotion": 0.2,
                "prob_other": 0.1,
            },
            "uri_2": {
                "prob_toxic": 0.3,
                "prob_constructive": 0.8,
                "is_sociopolitical": False,
                "is_not_sociopolitical": True,
                "is_political_left": False,
                "is_political_right": True,
                "is_political_moderate": False,
                "is_political_unclear": False,
                "prob_intergroup": 0.1,
                "prob_moral": 0.2,
                "prob_emotion": 0.3,
                "prob_other": 0.4,
            },
        }

        result = agg.get_labels_for_engaged_content(uris)

        assert set(result.keys()) == set(expected_result.keys())
        for uri, expected_labels in expected_result.items():
            for label_key, expected_value in expected_labels.items():
                assert label_key in result[uri]
                assert result[uri][label_key] == expected_value

    @patch(
        "services.calculate_analytics.study_analytics.get_user_engagement.get_agg_labels_for_engagements.get_partition_dates"
    )
    @patch(
        "services.calculate_analytics.study_analytics.get_user_engagement.get_agg_labels_for_engagements.get_labels_for_partition_date"
    )
    @patch(
        "services.calculate_analytics.study_analytics.get_user_engagement.get_agg_labels_for_engagements.get_relevant_probs_for_label"
    )
    @patch("builtins.print")
    def test_labels_aggregation_with_missing_integrations(
        self, mock_print, mock_probs, mock_labels, mock_dates
    ):
        """
        Test that the function handles URIs with missing integrations correctly.
        Input: Three URIs, where:
            - uri_1 has all three integrations
            - uri_2 is missing perspective_api
            - uri_3 is missing sociopolitical and ime
        Output: Dict mapping each URI to available label probabilities, with tracking of missing integrations.
        """
        uris = ["uri_1", "uri_2", "uri_3"]
        mock_dates.return_value = ["2024-10-01"]

        # Simulate three integrations with different coverage for each URI
        def labels_side_effect(integration, partition_date):
            if integration == "perspective_api":
                return pd.DataFrame(
                    [
                        {"uri": "uri_1", "prob_toxic": 0.7, "prob_constructive": 0.2},
                        # uri_2 is missing perspective_api
                        {"uri": "uri_3", "prob_toxic": 0.5, "prob_constructive": 0.5},
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
                        # uri_3 is missing sociopolitical
                    ]
                )
            elif integration == "ime":
                return pd.DataFrame(
                    [
                        {
                            "uri": "uri_1",
                            "prob_intergroup": 0.4,
                            "prob_moral": 0.3,
                            "prob_emotion": 0.2,
                            "prob_other": 0.1,
                        },
                        {
                            "uri": "uri_2",
                            "prob_intergroup": 0.1,
                            "prob_moral": 0.2,
                            "prob_emotion": 0.3,
                            "prob_other": 0.4,
                        },
                        # uri_3 is missing ime
                    ]
                )
            else:
                return pd.DataFrame([])

        mock_labels.side_effect = labels_side_effect

        def probs_side_effect(integration, label_dict):
            if integration == "perspective_api":
                return {
                    "prob_toxic": label_dict["prob_toxic"],
                    "prob_constructive": label_dict["prob_constructive"],
                }
            elif integration == "sociopolitical":
                return {
                    "is_sociopolitical": label_dict["is_sociopolitical"],
                    "is_not_sociopolitical": not label_dict["is_sociopolitical"],
                    "is_political_left": (
                        label_dict["political_ideology_label"] == "left"
                    ),
                    "is_political_right": (
                        label_dict["political_ideology_label"] == "right"
                    ),
                    "is_political_moderate": (
                        label_dict["political_ideology_label"] == "moderate"
                    ),
                    "is_political_unclear": (
                        label_dict["political_ideology_label"] == "unclear"
                    ),
                }
            elif integration == "ime":
                return {
                    "prob_intergroup": label_dict["prob_intergroup"],
                    "prob_moral": label_dict["prob_moral"],
                    "prob_emotion": label_dict["prob_emotion"],
                    "prob_other": label_dict["prob_other"],
                }
            else:
                return {}

        mock_probs.side_effect = probs_side_effect

        expected_result = {
            "uri_1": {
                "prob_toxic": 0.7,
                "prob_constructive": 0.2,
                "is_sociopolitical": True,
                "is_not_sociopolitical": False,
                "is_political_left": True,
                "is_political_right": False,
                "is_political_moderate": False,
                "is_political_unclear": False,
                "prob_intergroup": 0.4,
                "prob_moral": 0.3,
                "prob_emotion": 0.2,
                "prob_other": 0.1,
            },
            "uri_2": {
                "is_sociopolitical": False,
                "is_not_sociopolitical": True,
                "is_political_left": False,
                "is_political_right": True,
                "is_political_moderate": False,
                "is_political_unclear": False,
                "prob_intergroup": 0.1,
                "prob_moral": 0.2,
                "prob_emotion": 0.3,
                "prob_other": 0.4,
            },
            "uri_3": {
                "prob_toxic": 0.5,
                "prob_constructive": 0.5,
            },
        }

        # Expected missing integrations tracking
        expected_missing_integrations = {
            "perspective_api": ["uri_2"],
            "sociopolitical": ["uri_3"],
            "ime": ["uri_3"],
        }

        result = agg.get_labels_for_engaged_content(uris)

        # Verify the result contains all expected URIs
        assert set(result.keys()) == set(expected_result.keys())

        # Verify each URI has the expected labels
        for uri, expected_labels in expected_result.items():
            for label_key, expected_value in expected_labels.items():
                assert label_key in result[uri]
                assert result[uri][label_key] == expected_value

        # Verify the print statements about missing integrations
        mock_print.assert_any_call(
            "We have 2/3 still missing some integration of some sort."
        )
        mock_print.assert_any_call(
            f"integration_to_missing_uris={expected_missing_integrations}"
        )


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
        Input: Multiple users, multiple dates, various engagement patterns with multiple URIs.
        Output: Proportions for each label and record type.
        """
        labels_for_engaged_content = {
            "uri_1": {
                "prob_toxic": 0.8,
                "prob_constructive": 0.2,
                "is_sociopolitical": True,
                "is_not_sociopolitical": False,
                "is_political_left": True,
                "is_political_right": False,
                "is_political_moderate": False,
                "is_political_unclear": False,
                "prob_intergroup": 0.7,
                "prob_moral": 0.6,
                "prob_emotion": 0.5,
                "prob_other": 0.1,
            },
            "uri_2": {
                "prob_toxic": 0.3,
                "prob_constructive": 0.7,
                "is_sociopolitical": True,
                "is_not_sociopolitical": False,
                "is_political_left": False,
                "is_political_right": True,
                "is_political_moderate": False,
                "is_political_unclear": False,
                "prob_intergroup": 0.2,
                "prob_moral": 0.8,
                "prob_emotion": 0.3,
                "prob_other": 0.1,
            },
            "uri_3": {
                "prob_toxic": 0.1,
                "prob_constructive": 0.9,
                "is_sociopolitical": False,
                "is_not_sociopolitical": True,
                "is_political_left": False,
                "is_political_right": False,
                "is_political_moderate": False,
                "is_political_unclear": True,
                "prob_intergroup": 0.1,
                "prob_moral": 0.2,
                "prob_emotion": 0.9,
                "prob_other": 0.3,
            },
            "uri_4": {
                "prob_toxic": 0.5,
                "prob_constructive": 0.5,
                "is_sociopolitical": False,
                "is_not_sociopolitical": True,
                "is_political_left": False,
                "is_political_right": False,
                "is_political_moderate": True,
                "is_political_unclear": False,
                "prob_intergroup": 0.4,
                "prob_moral": 0.3,
                "prob_emotion": 0.2,
                "prob_other": 0.8,
            },
            "uri_5": {
                "prob_toxic": 0.9,
                "prob_constructive": 0.1,
                "is_sociopolitical": True,
                "is_not_sociopolitical": False,
                "is_political_left": False,
                "is_political_right": False,
                "is_political_moderate": False,
                "is_political_unclear": True,
                "prob_intergroup": 0.8,
                "prob_moral": 0.7,
                "prob_emotion": 0.6,
                "prob_other": 0.2,
            },
            "uri_6": {
                "prob_toxic": 0.2,
                "prob_constructive": 0.8,
                "is_sociopolitical": True,
                "is_not_sociopolitical": False,
                "is_political_left": False,
                "is_political_right": False,
                "is_political_moderate": True,
                "is_political_unclear": False,
                "prob_intergroup": 0.3,
                "prob_moral": 0.4,
                "prob_emotion": 0.7,
                "prob_other": 0.1,
            },
            "uri_7": {
                "prob_toxic": 0.4,
                "prob_constructive": 0.6,
                "is_sociopolitical": False,
                "is_not_sociopolitical": True,
                "is_political_left": False,
                "is_political_right": False,
                "is_political_moderate": False,
                "is_political_unclear": True,
                "prob_intergroup": 0.5,
                "prob_moral": 0.5,
                "prob_emotion": 0.5,
                "prob_other": 0.5,
            },
        }

        # Create test data for user engagement
        user_to_content_engaged_with = {
            "did_A": {
                "2024-10-01": {
                    "like": ["uri_1", "uri_2", "uri_3"],
                    "post": ["uri_4"],
                    "repost": [],
                    "reply": ["uri_5"],
                },
                "2024-10-02": {
                    "like": ["uri_6"],
                    "post": ["uri_7"],
                    "repost": ["uri_1"],
                    "reply": [],
                },
            },
            "did_B": {
                "2024-10-01": {
                    "like": ["uri_2", "uri_3"],
                    "post": [],
                    "repost": ["uri_4", "uri_5"],
                    "reply": ["uri_6"],
                },
                "2024-10-03": {
                    "like": [],
                    "post": ["uri_1"],
                    "repost": [],
                    "reply": ["uri_7"],
                },
            },
        }

        # Expected results for each user, date, and record type
        expected_result = {
            "did_A": {
                "2024-10-01": {
                    "prop_liked_posts_toxic": 0.333,  # 1 out of 3 (uri_1) has prob_toxic > 0.5
                    "prop_liked_posts_constructive": 0.667,  # 2 out of 3 (uri_2, uri_3) have prob_constructive > 0.5
                    "prop_liked_posts_sociopolitical": 0.667,  # 2 out of 3 are sociopolitical
                    "prop_liked_posts_is_not_sociopolitical": 0.333,  # 1 out of 3 is not sociopolitical
                    "prop_liked_posts_is_political_left": 0.333,  # 1 out of 3 is left
                    "prop_liked_posts_is_political_right": 0.333,  # 1 out of 3 is right
                    "prop_liked_posts_is_political_moderate": 0.000,  # 0 out of 3 are moderate
                    "prop_liked_posts_is_political_unclear": 0.333,  # 1 out of 3 is unclear
                    "prop_liked_posts_intergroup": 0.333,  # 1 out of 3 (uri_1) has prob_intergroup > 0.5
                    "prop_liked_posts_moral": 0.667,  # 2 out of 3 (uri_1, uri_2) have prob_moral > 0.5
                    "prop_liked_posts_emotion": 0.333,  # 1 out of 3 (uri_3) has prob_emotion > 0.5
                    "prop_liked_posts_other": 0.0,  # 0 out of 3 have prob_other > 0.5
                    "prop_posted_posts_toxic": 0.0,  # 0 out of 1 has prob_toxic > 0.5
                    "prop_posted_posts_constructive": 0.0,  # 0 out of 1 has prob_constructive > 0.5
                    "prop_posted_posts_sociopolitical": 0.0,  # 0 out of 1 is sociopolitical
                    "prop_posted_posts_is_not_sociopolitical": 1.0,  # 1 out of 1 is not sociopolitical
                    "prop_posted_posts_is_political_left": 0.0,  # 0 out of 1 is left
                    "prop_posted_posts_is_political_right": 0.0,  # 0 out of 1 is right
                    "prop_posted_posts_is_political_moderate": 1.0,  # 1 out of 1 is moderate
                    "prop_posted_posts_is_political_unclear": 0.0,  # 0 out of 1 is unclear
                    "prop_posted_posts_intergroup": 0.0,  # 0 out of 1 has prob_intergroup > 0.5
                    "prop_posted_posts_moral": 0.0,  # 0 out of 1 has prob_moral > 0.5
                    "prop_posted_posts_emotion": 0.0,  # 0 out of 1 has prob_emotion > 0.5
                    "prop_posted_posts_other": 1.0,  # 1 out of 1 has prob_other > 0.5
                    "prop_reposted_posts_toxic": None,  # No reposts
                    "prop_reposted_posts_constructive": None,
                    "prop_reposted_posts_sociopolitical": None,
                    "prop_reposted_posts_is_not_sociopolitical": None,
                    "prop_reposted_posts_is_political_left": None,
                    "prop_reposted_posts_is_political_right": None,
                    "prop_reposted_posts_is_political_moderate": None,
                    "prop_reposted_posts_is_political_unclear": None,
                    "prop_reposted_posts_intergroup": None,
                    "prop_reposted_posts_moral": None,
                    "prop_reposted_posts_emotion": None,
                    "prop_reposted_posts_other": None,
                    "prop_replied_posts_toxic": 1.0,  # 1 out of 1 has prob_toxic > 0.5
                    "prop_replied_posts_constructive": 0.0,  # 0 out of 1 has prob_constructive > 0.5
                    "prop_replied_posts_sociopolitical": 1.0,  # 1 out of 1 is sociopolitical
                    "prop_replied_posts_is_not_sociopolitical": 0.0,  # 0 out of 1 is not sociopolitical
                    "prop_replied_posts_is_political_left": 0.0,  # 0 out of 1 is left
                    "prop_replied_posts_is_political_right": 0.0,  # 0 out of 1 is right
                    "prop_replied_posts_is_political_moderate": 0.0,  # 0 out of 1 is moderate
                    "prop_replied_posts_is_political_unclear": 1.0,  # 1 out of 1 is unclear
                    "prop_replied_posts_intergroup": 1.0,  # 1 out of 1 has prob_intergroup > 0.5
                    "prop_replied_posts_moral": 1.0,  # 1 out of 1 has prob_moral > 0.5
                    "prop_replied_posts_emotion": 1.0,  # 1 out of 1 has prob_emotion > 0.5
                    "prop_replied_posts_other": 0.0,  # 0 out of 1 has prob_other > 0.5
                },
                "2024-10-02": {
                    "prop_liked_posts_toxic": 0.0,  # 0 out of 1 has prob_toxic > 0.5
                    "prop_liked_posts_constructive": 1.0,  # 1 out of 1 has prob_constructive > 0.5
                    "prop_liked_posts_sociopolitical": 1.0,  # 1 out of 1 is sociopolitical
                    "prop_liked_posts_is_not_sociopolitical": 0.0,  # 0 out of 1 is not sociopolitical
                    "prop_liked_posts_is_political_left": 0.0,  # 0 out of 1 is left
                    "prop_liked_posts_is_political_right": 0.0,  # 0 out of 1 is right
                    "prop_liked_posts_is_political_moderate": 1.0,  # 1 out of 1 is moderate
                    "prop_liked_posts_is_political_unclear": 0.0,  # 0 out of 1 is unclear
                    "prop_liked_posts_intergroup": 0.0,  # 0 out of 1 has prob_intergroup > 0.5
                    "prop_liked_posts_moral": 0.0,  # 0 out of 1 has prob_moral > 0.5
                    "prop_liked_posts_emotion": 1.0,  # 1 out of 1 has prob_emotion > 0.5
                    "prop_liked_posts_other": 0.0,  # 0 out of 1 has prob_other > 0.5
                    "prop_posted_posts_toxic": 0.0,  # 0 out of 1 has prob_toxic > 0.5
                    "prop_posted_posts_constructive": 1.0,  # 1 out of 1 has prob_constructive > 0.5
                    "prop_posted_posts_sociopolitical": 0.0,  # 0 out of 1 is sociopolitical
                    "prop_posted_posts_is_not_sociopolitical": 1.0,  # 1 out of 1 is not sociopolitical
                    "prop_posted_posts_is_political_left": 0.0,  # 0 out of 1 is left
                    "prop_posted_posts_is_political_right": 0.0,  # 0 out of 1 is right
                    "prop_posted_posts_is_political_moderate": 0.0,  # 0 out of 1 is moderate
                    "prop_posted_posts_is_political_unclear": 1.0,  # 1 out of 1 is unclear
                    "prop_posted_posts_intergroup": 0.0,  # 0 out of 1 has prob_intergroup > 0.5
                    "prop_posted_posts_moral": 0.0,  # 0 out of 1 has prob_moral > 0.5
                    "prop_posted_posts_emotion": 0.0,  # 0 out of 1 has prob_emotion > 0.5
                    "prop_posted_posts_other": 0.0,  # 0 out of 1 has prob_other > 0.5
                    "prop_reposted_posts_toxic": 1.0,  # 1 out of 1 has prob_toxic > 0.5
                    "prop_reposted_posts_constructive": 0.0,  # 0 out of 1 has prob_constructive > 0.5
                    "prop_reposted_posts_sociopolitical": 1.0,  # 1 out of 1 is sociopolitical
                    "prop_reposted_posts_is_not_sociopolitical": 0.0,  # 0 out of 1 is not sociopolitical
                    "prop_reposted_posts_is_political_left": 1.0,  # 1 out of 1 is left
                    "prop_reposted_posts_is_political_right": 0.0,  # 0 out of 1 is right
                    "prop_reposted_posts_is_political_moderate": 0.0,  # 0 out of 1 is moderate
                    "prop_reposted_posts_is_political_unclear": 0.0,  # 0 out of 1 is unclear
                    "prop_reposted_posts_intergroup": 1.0,  # 1 out of 1 has prob_intergroup > 0.5
                    "prop_reposted_posts_moral": 1.0,  # 1 out of 1 has prob_moral > 0.5
                    "prop_reposted_posts_emotion": 0.0,  # 0 out of 1 has prob_emotion > 0.5
                    "prop_reposted_posts_other": 0.0,  # 0 out of 1 has prob_other > 0.5
                    "prop_replied_posts_toxic": None,  # No replies
                    "prop_replied_posts_constructive": None,
                    "prop_replied_posts_sociopolitical": None,
                    "prop_replied_posts_is_not_sociopolitical": None,
                    "prop_replied_posts_is_political_left": None,
                    "prop_replied_posts_is_political_right": None,
                    "prop_replied_posts_is_political_moderate": None,
                    "prop_replied_posts_is_political_unclear": None,
                    "prop_replied_posts_intergroup": None,
                    "prop_replied_posts_moral": None,
                    "prop_replied_posts_emotion": None,
                    "prop_replied_posts_other": None,
                },
            },
            "did_B": {
                "2024-10-01": {
                    "prop_liked_posts_toxic": 0.0,  # 0 out of 2 have prob_toxic > 0.5
                    "prop_liked_posts_constructive": 1.0,  # 2 out of 2 have prob_constructive > 0.5
                    "prop_liked_posts_sociopolitical": 0.5,  # 1 out of 2 is sociopolitical
                    "prop_liked_posts_is_not_sociopolitical": 0.5,  # 1 out of 2 is not sociopolitical
                    "prop_liked_posts_is_political_left": 0.0,  # 0 out of 2 is left
                    "prop_liked_posts_is_political_right": 0.5,  # 1 out of 2 is right
                    "prop_liked_posts_is_political_moderate": 0.0,  # 0 out of 2 is moderate
                    "prop_liked_posts_is_political_unclear": 0.5,  # 1 out of 2 is unclear
                    "prop_liked_posts_intergroup": 0.0,  # 0 out of 2 have prob_intergroup > 0.5
                    "prop_liked_posts_moral": 0.5,  # 1 out of 2 (uri_2) has prob_moral > 0.5
                    "prop_liked_posts_emotion": 0.5,  # 1 out of 2 (uri_3) has prob_emotion > 0.5
                    "prop_liked_posts_other": 0.0,  # 0 out of 2 have prob_other > 0.5
                    "prop_posted_posts_toxic": None,  # No posts
                    "prop_posted_posts_constructive": None,
                    "prop_posted_posts_sociopolitical": None,
                    "prop_posted_posts_is_not_sociopolitical": None,
                    "prop_posted_posts_is_political_left": None,
                    "prop_posted_posts_is_political_right": None,
                    "prop_posted_posts_is_political_moderate": None,
                    "prop_posted_posts_is_political_unclear": None,
                    "prop_posted_posts_intergroup": None,
                    "prop_posted_posts_moral": None,
                    "prop_posted_posts_emotion": None,
                    "prop_posted_posts_other": None,
                    "prop_reposted_posts_toxic": 0.5,  # 1 out of 2 (uri_5) has prob_toxic > 0.5
                    "prop_reposted_posts_constructive": 0.0,  # 0 out of 2 have prob_constructive > 0.5
                    "prop_reposted_posts_sociopolitical": 0.5,  # 1 out of 2 is sociopolitical
                    "prop_reposted_posts_is_not_sociopolitical": 0.5,  # 1 out of 2 is not sociopolitical
                    "prop_reposted_posts_is_political_left": 0.0,  # 0 out of 2 is left
                    "prop_reposted_posts_is_political_right": 0.0,  # 0 out of 2 is right
                    "prop_reposted_posts_is_political_moderate": 0.5,  # 1 out of 2 is moderate
                    "prop_reposted_posts_is_political_unclear": 0.5,  # 1 out of 2 is unclear
                    "prop_reposted_posts_intergroup": 0.5,  # 1 out of 2 (uri_5) has prob_intergroup > 0.5
                    "prop_reposted_posts_moral": 0.5,  # 1 out of 2 (uri_5) has prob_moral > 0.5
                    "prop_reposted_posts_emotion": 0.5,  # 1 out of 2 (uri_5) has prob_emotion > 0.5
                    "prop_reposted_posts_other": 0.5,  # 1 out of 2 (uri_4) has prob_other > 0.5
                    "prop_replied_posts_toxic": 0.0,  # 0 out of 1 has prob_toxic > 0.5
                    "prop_replied_posts_constructive": 1.0,  # 1 out of 1 has prob_constructive > 0.5
                    "prop_replied_posts_sociopolitical": 1.0,  # 1 out of 1 is sociopolitical
                    "prop_replied_posts_is_not_sociopolitical": 0.0,  # 0 out of 1 is not sociopolitical
                    "prop_replied_posts_is_political_left": 0.0,  # 0 out of 1 is left
                    "prop_replied_posts_is_political_right": 0.0,  # 0 out of 1 is right
                    "prop_replied_posts_is_political_moderate": 1.0,  # 1 out of 1 is moderate
                    "prop_replied_posts_is_political_unclear": 0.0,  # 0 out of 1 is unclear
                    "prop_replied_posts_intergroup": 0.0,  # 0 out of 1 has prob_intergroup > 0.5
                    "prop_replied_posts_moral": 0.0,  # 0 out of 1 has prob_moral > 0.5
                    "prop_replied_posts_emotion": 1.0,  # 1 out of 1 has prob_emotion > 0.5
                    "prop_replied_posts_other": 0.0,  # 0 out of 1 has prob_other > 0.5
                },
                "2024-10-03": {
                    "prop_liked_posts_toxic": None,  # No likes
                    "prop_liked_posts_constructive": None,
                    "prop_liked_posts_sociopolitical": None,
                    "prop_liked_posts_is_not_sociopolitical": None,
                    "prop_liked_posts_is_political_left": None,
                    "prop_liked_posts_is_political_right": None,
                    "prop_liked_posts_is_political_moderate": None,
                    "prop_liked_posts_is_political_unclear": None,
                    "prop_liked_posts_intergroup": None,
                    "prop_liked_posts_moral": None,
                    "prop_liked_posts_emotion": None,
                    "prop_liked_posts_other": None,
                    "prop_posted_posts_toxic": 1.0,  # 1 out of 1 has prob_toxic > 0.5
                    "prop_posted_posts_constructive": 0.0,  # 0 out of 1 has prob_constructive > 0.5
                    "prop_posted_posts_sociopolitical": 1.0,  # 1 out of 1 is sociopolitical
                    "prop_posted_posts_is_not_sociopolitical": 0.0,  # 0 out of 1 is not sociopolitical
                    "prop_posted_posts_is_political_left": 1.0,  # 1 out of 1 is left
                    "prop_posted_posts_is_political_right": 0.0,  # 0 out of 1 is right
                    "prop_posted_posts_is_political_moderate": 0.0,  # 0 out of 1 is moderate
                    "prop_posted_posts_is_political_unclear": 0.0,  # 0 out of 1 is unclear
                    "prop_posted_posts_intergroup": 1.0,  # 1 out of 1 has prob_intergroup > 0.5
                    "prop_posted_posts_moral": 1.0,  # 1 out of 1 has prob_moral > 0.5
                    "prop_posted_posts_emotion": 0.0,  # 0 out of 1 has prob_emotion > 0.5
                    "prop_posted_posts_other": 0.0,  # 0 out of 1 has prob_other > 0.5
                    "prop_reposted_posts_toxic": None,  # No reposts
                    "prop_reposted_posts_constructive": None,
                    "prop_reposted_posts_sociopolitical": None,
                    "prop_reposted_posts_is_not_sociopolitical": None,
                    "prop_reposted_posts_is_political_left": None,
                    "prop_reposted_posts_is_political_right": None,
                    "prop_reposted_posts_is_political_moderate": None,
                    "prop_reposted_posts_is_political_unclear": None,
                    "prop_reposted_posts_intergroup": None,
                    "prop_reposted_posts_moral": None,
                    "prop_reposted_posts_emotion": None,
                    "prop_reposted_posts_other": None,
                    "prop_replied_posts_toxic": 0.0,  # 0 out of 1 has prob_toxic > 0.5
                    "prop_replied_posts_constructive": 1.0,  # 1 out of 1 has prob_constructive > 0.5
                    "prop_replied_posts_sociopolitical": 0.0,  # 0 out of 1 is sociopolitical
                    "prop_replied_posts_is_not_sociopolitical": 1.0,  # 1 out of 1 is not sociopolitical
                    "prop_replied_posts_is_political_left": 0.0,  # 0 out of 1 is left
                    "prop_replied_posts_is_political_right": 0.0,  # 0 out of 1 is right
                    "prop_replied_posts_is_political_moderate": 0.0,  # 0 out of 1 is moderate
                    "prop_replied_posts_is_political_unclear": 1.0,  # 1 out of 1 is unclear
                    "prop_replied_posts_intergroup": 0.0,  # 0 out of 1 has prob_intergroup > 0.5
                    "prop_replied_posts_moral": 0.0,  # 0 out of 1 has prob_moral > 0.5
                    "prop_replied_posts_emotion": 0.0,  # 0 out of 1 has prob_emotion > 0.5
                    "prop_replied_posts_other": 0.0,  # 0 out of 1 has prob_other > 0.5
                },
            },
        }

        result = agg.get_per_user_per_day_content_label_proportions(
            user_to_content_engaged_with=user_to_content_engaged_with,
            labels_for_engaged_content=labels_for_engaged_content,
        )

        # Verify the result structure
        assert set(result.keys()) == set(expected_result.keys())

        # Verify all results for all users and dates
        for user_did in expected_result:
            assert user_did in result, f"User {user_did} missing from results"

            for test_date in expected_result[user_did]:
                assert (
                    test_date in result[user_did]
                ), f"Date {test_date} missing for user {user_did}"

                # Verify all metrics for this user and date
                for metric, expected_value in expected_result[user_did][
                    test_date
                ].items():
                    assert (
                        metric in result[user_did][test_date]
                    ), f"Metric {metric} missing for user {user_did} on {test_date}"

                    if expected_value is None:
                        assert (
                            result[user_did][test_date][metric] is None
                        ), f"Expected None for {metric} but got {result[user_did][test_date][metric]}"
                    else:
                        assert (
                            result[user_did][test_date][metric]
                            == pytest.approx(expected_value, abs=0.001)
                        ), f"Metric {metric} for user {user_did} on {test_date}: expected {expected_value}, got {result[user_did][test_date][metric]}"


class TestGetPerUserToWeeklyContentLabelProportions:
    """
    Unit tests for get_per_user_to_weekly_content_label_proportions.
    This function aggregates daily content label proportions into weekly averages.
    """

    def test_weekly_aggregation_comprehensive(self):
        """
        Test that the function correctly aggregates daily proportions into weekly values.

        This test involves:
        - 2 users (did_A and did_B)
        - Multiple posts with various content labels (toxic, constructive, sociopolitical, etc.)
        - 4 engagement types (likes, posts, replies, reposts)
        - 3 weeks of data with varying days per week:
          * User A has 3 days in week 1 (Oct 1-3) and 1 day in week 3 (Oct 15)
          * User B has 2 days in week 1 (Oct 1, Oct 3)
          * User A has no data in week 2, User B has no data in weeks 2 and 3.

        Expected behavior:
        - Daily proportions should be averaged to create weekly metrics
        - For User A's week 1, metrics should be the average of 3 days of data
        - For User B's week 1, metrics should be the average of 2 days of data
        - For User A's week 3, metrics should reflect the single day's data
        - For weeks with no data (User A's week 2), all metrics should be None
        - When a user has no data for a particular engagement type on a day (e.g., User A has no repost data on Oct 1 and Oct 3),
          those days should be excluded from the weekly average calculation
        - All content label categories should be properly aggregated (toxic, constructive, political orientation, etc.)
        """
        # Use the same structure as in TestGetPerUserPerDayContentLabelProportions
        # but organize by weeks
        user_per_day_content_label_proportions = {
            "did_A": {
                # Week 1 - 3 days
                "2024-10-01": {
                    "prop_liked_posts_toxic": 0.333,
                    "prop_liked_posts_constructive": 0.667,
                    "prop_liked_posts_sociopolitical": 0.667,
                    "prop_liked_posts_is_not_sociopolitical": 0.333,
                    "prop_liked_posts_is_political_left": 0.333,
                    "prop_liked_posts_is_political_right": 0.333,
                    "prop_liked_posts_is_political_moderate": 0.000,
                    "prop_liked_posts_is_political_unclear": 0.333,
                    "prop_liked_posts_intergroup": 0.333,
                    "prop_liked_posts_moral": 0.333,
                    "prop_liked_posts_emotion": 0.333,
                    "prop_liked_posts_other": 0.000,
                    "prop_posted_posts_toxic": 0.0,
                    "prop_posted_posts_constructive": 1.0,
                    "prop_posted_posts_sociopolitical": 0.0,
                    "prop_posted_posts_is_not_sociopolitical": 1.0,
                    "prop_posted_posts_is_political_left": 0.0,
                    "prop_posted_posts_is_political_right": 0.0,
                    "prop_posted_posts_is_political_moderate": 1.0,
                    "prop_posted_posts_is_political_unclear": 0.0,
                    "prop_posted_posts_intergroup": 0.0,
                    "prop_posted_posts_moral": 0.0,
                    "prop_posted_posts_emotion": 0.0,
                    "prop_posted_posts_other": 1.0,
                    "prop_reposted_posts_toxic": None,
                    "prop_reposted_posts_constructive": None,
                    "prop_reposted_posts_sociopolitical": None,
                    "prop_reposted_posts_is_not_sociopolitical": None,
                    "prop_reposted_posts_is_political_left": None,
                    "prop_reposted_posts_is_political_right": None,
                    "prop_reposted_posts_is_political_moderate": None,
                    "prop_reposted_posts_is_political_unclear": None,
                    "prop_reposted_posts_intergroup": None,
                    "prop_reposted_posts_moral": None,
                    "prop_reposted_posts_emotion": None,
                    "prop_reposted_posts_other": None,
                    "prop_replied_posts_toxic": 1.0,
                    "prop_replied_posts_constructive": 0.0,
                    "prop_replied_posts_sociopolitical": 1.0,
                    "prop_replied_posts_is_not_sociopolitical": 0.0,
                    "prop_replied_posts_is_political_left": 0.0,
                    "prop_replied_posts_is_political_right": 0.0,
                    "prop_replied_posts_is_political_moderate": 0.0,
                    "prop_replied_posts_is_political_unclear": 1.0,
                    "prop_replied_posts_intergroup": 1.0,
                    "prop_replied_posts_moral": 1.0,
                    "prop_replied_posts_emotion": 1.0,
                    "prop_replied_posts_other": 0.0,
                },
                "2024-10-02": {
                    "prop_liked_posts_toxic": 0.5,
                    "prop_liked_posts_constructive": 0.5,
                    "prop_liked_posts_sociopolitical": 0.5,
                    "prop_liked_posts_is_not_sociopolitical": 0.5,
                    "prop_liked_posts_is_political_left": 0.0,
                    "prop_liked_posts_is_political_right": 0.0,
                    "prop_liked_posts_is_political_moderate": 1.0,
                    "prop_liked_posts_is_political_unclear": 0.0,
                    "prop_liked_posts_intergroup": 0.0,
                    "prop_liked_posts_moral": 0.0,
                    "prop_liked_posts_emotion": 1.0,
                    "prop_liked_posts_other": 0.0,
                    "prop_posted_posts_toxic": 0.25,
                    "prop_posted_posts_constructive": 0.75,
                    "prop_posted_posts_sociopolitical": 0.5,
                    "prop_posted_posts_is_not_sociopolitical": 0.5,
                    "prop_posted_posts_is_political_left": 0.0,
                    "prop_posted_posts_is_political_right": 0.0,
                    "prop_posted_posts_is_political_moderate": 0.5,
                    "prop_posted_posts_is_political_unclear": 0.5,
                    "prop_posted_posts_intergroup": 0.5,
                    "prop_posted_posts_moral": 0.5,
                    "prop_posted_posts_emotion": 0.5,
                    "prop_posted_posts_other": 0.5,
                    "prop_reposted_posts_toxic": 1.0,
                    "prop_reposted_posts_constructive": 0.0,
                    "prop_reposted_posts_sociopolitical": 1.0,
                    "prop_reposted_posts_is_not_sociopolitical": 0.0,
                    "prop_reposted_posts_is_political_left": 1.0,
                    "prop_reposted_posts_is_political_right": 0.0,
                    "prop_reposted_posts_is_political_moderate": 0.0,
                    "prop_reposted_posts_is_political_unclear": 0.0,
                    "prop_reposted_posts_intergroup": 1.0,
                    "prop_reposted_posts_moral": 1.0,
                    "prop_reposted_posts_emotion": 0.0,
                    "prop_reposted_posts_other": 0.0,
                    "prop_replied_posts_toxic": None,
                    "prop_replied_posts_constructive": None,
                    "prop_replied_posts_sociopolitical": None,
                    "prop_replied_posts_is_not_sociopolitical": None,
                    "prop_replied_posts_is_political_left": None,
                    "prop_replied_posts_is_political_right": None,
                    "prop_replied_posts_is_political_moderate": None,
                    "prop_replied_posts_is_political_unclear": None,
                    "prop_replied_posts_intergroup": None,
                    "prop_replied_posts_moral": None,
                    "prop_replied_posts_emotion": None,
                    "prop_replied_posts_other": None,
                },
                "2024-10-03": {
                    "prop_liked_posts_toxic": 0.667,
                    "prop_liked_posts_constructive": 0.333,
                    "prop_liked_posts_sociopolitical": 0.333,
                    "prop_liked_posts_is_not_sociopolitical": 0.667,
                    "prop_liked_posts_is_political_left": 0.0,
                    "prop_liked_posts_is_political_right": 0.0,
                    "prop_liked_posts_is_political_moderate": 0.0,
                    "prop_liked_posts_is_political_unclear": 1.0,
                    "prop_liked_posts_intergroup": 0.333,
                    "prop_liked_posts_moral": 0.333,
                    "prop_liked_posts_emotion": 0.667,
                    "prop_liked_posts_other": 0.333,
                    "prop_posted_posts_toxic": 0.5,
                    "prop_posted_posts_constructive": 0.5,
                    "prop_posted_posts_sociopolitical": 0.0,
                    "prop_posted_posts_is_not_sociopolitical": 1.0,
                    "prop_posted_posts_is_political_left": 0.0,
                    "prop_posted_posts_is_political_right": 0.0,
                    "prop_posted_posts_is_political_moderate": 0.0,
                    "prop_posted_posts_is_political_unclear": 1.0,
                    "prop_posted_posts_intergroup": 0.0,
                    "prop_posted_posts_moral": 0.0,
                    "prop_posted_posts_emotion": 0.0,
                    "prop_posted_posts_other": 0.0,
                    "prop_reposted_posts_toxic": None,
                    "prop_reposted_posts_constructive": None,
                    "prop_reposted_posts_sociopolitical": None,
                    "prop_reposted_posts_is_not_sociopolitical": None,
                    "prop_reposted_posts_is_political_left": None,
                    "prop_reposted_posts_is_political_right": None,
                    "prop_reposted_posts_is_political_moderate": None,
                    "prop_reposted_posts_is_political_unclear": None,
                    "prop_reposted_posts_intergroup": None,
                    "prop_reposted_posts_moral": None,
                    "prop_reposted_posts_emotion": None,
                    "prop_reposted_posts_other": None,
                    "prop_replied_posts_toxic": 0.0,
                    "prop_replied_posts_constructive": 1.0,
                    "prop_replied_posts_sociopolitical": 0.0,
                    "prop_replied_posts_is_not_sociopolitical": 1.0,
                    "prop_replied_posts_is_political_left": 0.0,
                    "prop_replied_posts_is_political_right": 0.0,
                    "prop_replied_posts_is_political_moderate": 0.0,
                    "prop_replied_posts_is_political_unclear": 1.0,
                    "prop_replied_posts_intergroup": 0.0,
                    "prop_replied_posts_moral": 0.0,
                    "prop_replied_posts_emotion": 0.0,
                    "prop_replied_posts_other": 0.0,
                },
                # Week 2 - 0 days (no data)
                # Week 3 - 1 day
                "2024-10-15": {
                    "prop_liked_posts_toxic": 0.8,
                    "prop_liked_posts_constructive": 0.2,
                    "prop_liked_posts_sociopolitical": 1.0,
                    "prop_liked_posts_is_not_sociopolitical": 0.0,
                    "prop_liked_posts_is_political_left": 0.5,
                    "prop_liked_posts_is_political_right": 0.5,
                    "prop_liked_posts_is_political_moderate": 0.0,
                    "prop_liked_posts_is_political_unclear": 0.0,
                    "prop_liked_posts_intergroup": 0.8,
                    "prop_liked_posts_moral": 0.6,
                    "prop_liked_posts_emotion": 0.4,
                    "prop_liked_posts_other": 0.2,
                    "prop_posted_posts_toxic": 0.75,
                    "prop_posted_posts_constructive": 0.25,
                    "prop_posted_posts_sociopolitical": 1.0,
                    "prop_posted_posts_is_not_sociopolitical": 0.0,
                    "prop_posted_posts_is_political_left": 1.0,
                    "prop_posted_posts_is_political_right": 0.0,
                    "prop_posted_posts_is_political_moderate": 0.0,
                    "prop_posted_posts_is_political_unclear": 0.0,
                    "prop_posted_posts_intergroup": 0.75,
                    "prop_posted_posts_moral": 0.75,
                    "prop_posted_posts_emotion": 0.25,
                    "prop_posted_posts_other": 0.0,
                    "prop_reposted_posts_toxic": None,
                    "prop_reposted_posts_constructive": None,
                    "prop_reposted_posts_sociopolitical": None,
                    "prop_reposted_posts_is_not_sociopolitical": None,
                    "prop_reposted_posts_is_political_left": None,
                    "prop_reposted_posts_is_political_right": None,
                    "prop_reposted_posts_is_political_moderate": None,
                    "prop_reposted_posts_is_political_unclear": None,
                    "prop_reposted_posts_intergroup": None,
                    "prop_reposted_posts_moral": None,
                    "prop_reposted_posts_emotion": None,
                    "prop_reposted_posts_other": None,
                    "prop_replied_posts_toxic": None,
                    "prop_replied_posts_constructive": None,
                    "prop_replied_posts_sociopolitical": None,
                    "prop_replied_posts_is_not_sociopolitical": None,
                    "prop_replied_posts_is_political_left": None,
                    "prop_replied_posts_is_political_right": None,
                    "prop_replied_posts_is_political_moderate": None,
                    "prop_replied_posts_is_political_unclear": None,
                    "prop_replied_posts_intergroup": None,
                    "prop_replied_posts_moral": None,
                    "prop_replied_posts_emotion": None,
                    "prop_replied_posts_other": None,
                },
            },
            "did_B": {
                # Only has data for Week 1
                "2024-10-01": {
                    "prop_liked_posts_toxic": 0.4,
                    "prop_liked_posts_constructive": 0.6,
                    "prop_liked_posts_sociopolitical": 0.7,
                    "prop_liked_posts_is_not_sociopolitical": 0.3,
                    "prop_liked_posts_is_political_left": 0.2,
                    "prop_liked_posts_is_political_right": 0.3,
                    "prop_liked_posts_is_political_moderate": 0.0,
                    "prop_liked_posts_is_political_unclear": 0.5,
                    "prop_liked_posts_intergroup": 0.2,
                    "prop_liked_posts_moral": 0.5,
                    "prop_liked_posts_emotion": 0.4,
                    "prop_liked_posts_other": 0.1,
                    "prop_posted_posts_toxic": 0.2,
                    "prop_posted_posts_constructive": 0.8,
                    "prop_posted_posts_sociopolitical": 0.6,
                    "prop_posted_posts_is_not_sociopolitical": 0.4,
                    "prop_posted_posts_is_political_left": 0.3,
                    "prop_posted_posts_is_political_right": 0.1,
                    "prop_posted_posts_is_political_moderate": 0.2,
                    "prop_posted_posts_is_political_unclear": 0.4,
                    "prop_posted_posts_intergroup": 0.3,
                    "prop_posted_posts_moral": 0.4,
                    "prop_posted_posts_emotion": 0.5,
                    "prop_posted_posts_other": 0.2,
                    "prop_reposted_posts_toxic": 0.5,
                    "prop_reposted_posts_constructive": 0.5,
                    "prop_reposted_posts_sociopolitical": 0.5,
                    "prop_reposted_posts_is_not_sociopolitical": 0.5,
                    "prop_reposted_posts_is_political_left": 0.0,
                    "prop_reposted_posts_is_political_right": 0.0,
                    "prop_reposted_posts_is_political_moderate": 0.5,
                    "prop_reposted_posts_is_political_unclear": 0.5,
                    "prop_reposted_posts_intergroup": 0.5,
                    "prop_reposted_posts_moral": 0.5,
                    "prop_reposted_posts_emotion": 0.5,
                    "prop_reposted_posts_other": 0.5,
                    "prop_replied_posts_toxic": 0.0,
                    "prop_replied_posts_constructive": 1.0,
                    "prop_replied_posts_sociopolitical": 1.0,
                    "prop_replied_posts_is_not_sociopolitical": 0.0,
                    "prop_replied_posts_is_political_left": 0.0,
                    "prop_replied_posts_is_political_right": 0.0,
                    "prop_replied_posts_is_political_moderate": 1.0,
                    "prop_replied_posts_is_political_unclear": 0.0,
                    "prop_replied_posts_intergroup": 0.0,
                    "prop_replied_posts_moral": 0.0,
                    "prop_replied_posts_emotion": 1.0,
                    "prop_replied_posts_other": 0.0,
                },
                "2024-10-03": {
                    "prop_liked_posts_toxic": 0.6,
                    "prop_liked_posts_constructive": 0.4,
                    "prop_liked_posts_sociopolitical": 0.3,
                    "prop_liked_posts_is_not_sociopolitical": 0.7,
                    "prop_liked_posts_is_political_left": 0.1,
                    "prop_liked_posts_is_political_right": 0.1,
                    "prop_liked_posts_is_political_moderate": 0.1,
                    "prop_liked_posts_is_political_unclear": 0.7,
                    "prop_liked_posts_intergroup": 0.3,
                    "prop_liked_posts_moral": 0.2,
                    "prop_liked_posts_emotion": 0.6,
                    "prop_liked_posts_other": 0.4,
                    "prop_posted_posts_toxic": 0.3,
                    "prop_posted_posts_constructive": 0.7,
                    "prop_posted_posts_sociopolitical": 0.4,
                    "prop_posted_posts_is_not_sociopolitical": 0.6,
                    "prop_posted_posts_is_political_left": 0.2,
                    "prop_posted_posts_is_political_right": 0.0,
                    "prop_posted_posts_is_political_moderate": 0.0,
                    "prop_posted_posts_is_political_unclear": 0.8,
                    "prop_posted_posts_intergroup": 0.2,
                    "prop_posted_posts_moral": 0.3,
                    "prop_posted_posts_emotion": 0.7,
                    "prop_posted_posts_other": 0.3,
                    "prop_reposted_posts_toxic": None,
                    "prop_reposted_posts_constructive": None,
                    "prop_reposted_posts_sociopolitical": None,
                    "prop_reposted_posts_is_not_sociopolitical": None,
                    "prop_reposted_posts_is_political_left": None,
                    "prop_reposted_posts_is_political_right": None,
                    "prop_reposted_posts_is_political_moderate": None,
                    "prop_reposted_posts_is_political_unclear": None,
                    "prop_reposted_posts_intergroup": None,
                    "prop_reposted_posts_moral": None,
                    "prop_reposted_posts_emotion": None,
                    "prop_reposted_posts_other": None,
                    "prop_replied_posts_toxic": 0.0,
                    "prop_replied_posts_constructive": 1.0,
                    "prop_replied_posts_sociopolitical": 0.0,
                    "prop_replied_posts_is_not_sociopolitical": 1.0,
                    "prop_replied_posts_is_political_left": 0.0,
                    "prop_replied_posts_is_political_right": 0.0,
                    "prop_replied_posts_is_political_moderate": 0.0,
                    "prop_replied_posts_is_political_unclear": 1.0,
                    "prop_replied_posts_intergroup": 0.0,
                    "prop_replied_posts_moral": 0.0,
                    "prop_replied_posts_emotion": 0.0,
                    "prop_replied_posts_other": 0.0,
                },
            },
        }

        # Create a mapping from dates to weeks
        user_date_to_week_df = pd.DataFrame(
            [
                # User A
                {"bluesky_user_did": "did_A", "date": "2024-10-01", "week": "week_1"},
                {"bluesky_user_did": "did_A", "date": "2024-10-02", "week": "week_1"},
                {"bluesky_user_did": "did_A", "date": "2024-10-03", "week": "week_1"},
                {"bluesky_user_did": "did_A", "date": "2024-10-15", "week": "week_3"},
                # User B - only has data for week 1
                {"bluesky_user_did": "did_B", "date": "2024-10-01", "week": "week_1"},
                {"bluesky_user_did": "did_B", "date": "2024-10-03", "week": "week_1"},
                {"bluesky_user_did": "did_B", "date": "2024-10-08", "week": "week_2"},
                {"bluesky_user_did": "did_B", "date": "2024-10-15", "week": "week_3"},
            ]
        )

        # Expected weekly aggregations
        expected_result = {
            "did_A": {
                "week_1": {
                    # Average of 0.333, 0.5, and 0.667
                    "prop_liked_posts_toxic": 0.5,
                    # Average of 0.667, 0.5, and 0.333
                    "prop_liked_posts_constructive": 0.5,
                    # Average of 0.667, 0.5, and 0.333
                    "prop_liked_posts_sociopolitical": 0.5,
                    # Average of 0.333, 0.5, and 0.667
                    "prop_liked_posts_is_not_sociopolitical": 0.5,
                    # Average of 0.333, 0.0, 0.0, 0.0
                    "prop_liked_posts_is_political_left": 0.111,
                    # Average of 0.333, 0.0, 0.0
                    "prop_liked_posts_is_political_right": 0.111,
                    # Average of 0.0, 1.0, 0.0
                    "prop_liked_posts_is_political_moderate": 0.333,
                    # Average of 0.333, 0.0, 1.0
                    "prop_liked_posts_is_political_unclear": 0.444,
                    # Average of 0.333, 0.0, 0.333
                    "prop_liked_posts_intergroup": 0.222,
                    # Average of 0.333, 0.0, 0.333
                    "prop_liked_posts_moral": 0.222,
                    # Average of 0.333, 1.0, 0.667
                    "prop_liked_posts_emotion": 0.667,
                    # Average of 0.0, 0.0, 0.333
                    "prop_liked_posts_other": 0.111,
                    # Average of 0.0, 0.25, and 0.5
                    "prop_posted_posts_toxic": 0.25,
                    # Average of 1.0, 0.75, and 0.5
                    "prop_posted_posts_constructive": 0.75,
                    # Average of 0.0, 0.5, 0.0
                    "prop_posted_posts_sociopolitical": 0.167,
                    # Average of 1.0, 0.5, 1.0
                    "prop_posted_posts_is_not_sociopolitical": 0.833,
                    # Average of 0.0, 0.0, 0.0
                    "prop_posted_posts_is_political_left": 0.0,
                    # Average of 0.0, 0.0, 0.0
                    "prop_posted_posts_is_political_right": 0.0,
                    # Average of 1.0, 0.5, 0.0
                    "prop_posted_posts_is_political_moderate": 0.5,
                    # Average of 0.0, 0.5, 1.0
                    "prop_posted_posts_is_political_unclear": 0.5,
                    # Average of 0.0, 0.5, 0.0
                    "prop_posted_posts_intergroup": 0.167,
                    # Average of 0.0, 0.5, 0.0
                    "prop_posted_posts_moral": 0.167,
                    # Average of 0.0, 0.5, 0.0
                    "prop_posted_posts_emotion": 0.167,
                    # Average of 1.0, 0.5, 0.0
                    "prop_posted_posts_other": 0.5,
                    # Only day 2 has data
                    "prop_reposted_posts_toxic": 1.0,
                    "prop_reposted_posts_constructive": 0.0,
                    "prop_reposted_posts_sociopolitical": 1.0,
                    "prop_reposted_posts_is_not_sociopolitical": 0.0,
                    "prop_reposted_posts_is_political_left": 1.0,
                    "prop_reposted_posts_is_political_right": 0.0,
                    "prop_reposted_posts_is_political_moderate": 0.0,
                    "prop_reposted_posts_is_political_unclear": 0.0,
                    "prop_reposted_posts_intergroup": 1.0,
                    "prop_reposted_posts_moral": 1.0,
                    "prop_reposted_posts_emotion": 0.0,
                    "prop_reposted_posts_other": 0.0,
                    # Average of days 1 and 3 (day 2 is None)
                    "prop_replied_posts_toxic": 0.5,
                    "prop_replied_posts_constructive": 0.5,
                    "prop_replied_posts_sociopolitical": 0.5,
                    "prop_replied_posts_is_not_sociopolitical": 0.5,
                    "prop_replied_posts_is_political_left": 0.0,
                    "prop_replied_posts_is_political_right": 0.0,
                    "prop_replied_posts_is_political_moderate": 0.0,
                    "prop_replied_posts_is_political_unclear": 1.0,
                    "prop_replied_posts_intergroup": 0.5,
                    "prop_replied_posts_moral": 0.5,
                    "prop_replied_posts_emotion": 0.5,
                    "prop_replied_posts_other": 0.0,
                },  # no data for Week 2.
                "week_3": {
                    # Only one day of data
                    "prop_liked_posts_toxic": 0.8,
                    "prop_liked_posts_constructive": 0.2,
                    "prop_liked_posts_sociopolitical": 1.0,
                    "prop_liked_posts_is_not_sociopolitical": 0.0,
                    "prop_liked_posts_is_political_left": 0.5,
                    "prop_liked_posts_is_political_right": 0.5,
                    "prop_liked_posts_is_political_moderate": 0.0,
                    "prop_liked_posts_is_political_unclear": 0.0,
                    "prop_liked_posts_intergroup": 0.8,
                    "prop_liked_posts_moral": 0.6,
                    "prop_liked_posts_emotion": 0.4,
                    "prop_liked_posts_other": 0.2,
                    "prop_posted_posts_toxic": 0.75,
                    "prop_posted_posts_constructive": 0.25,
                    "prop_posted_posts_sociopolitical": 1.0,
                    "prop_posted_posts_is_not_sociopolitical": 0.0,
                    "prop_posted_posts_is_political_left": 1.0,
                    "prop_posted_posts_is_political_right": 0.0,
                    "prop_posted_posts_is_political_moderate": 0.0,
                    "prop_posted_posts_is_political_unclear": 0.0,
                    "prop_posted_posts_intergroup": 0.75,
                    "prop_posted_posts_moral": 0.75,
                    "prop_posted_posts_emotion": 0.25,
                    "prop_posted_posts_other": 0.0,
                    "prop_reposted_posts_toxic": None,
                    "prop_reposted_posts_constructive": None,
                    "prop_reposted_posts_sociopolitical": None,
                    "prop_reposted_posts_is_not_sociopolitical": None,
                    "prop_reposted_posts_is_political_left": None,
                    "prop_reposted_posts_is_political_right": None,
                    "prop_reposted_posts_is_political_moderate": None,
                    "prop_reposted_posts_is_political_unclear": None,
                    "prop_reposted_posts_intergroup": None,
                    "prop_reposted_posts_moral": None,
                    "prop_reposted_posts_emotion": None,
                    "prop_reposted_posts_other": None,
                    "prop_replied_posts_toxic": None,
                    "prop_replied_posts_constructive": None,
                    "prop_replied_posts_sociopolitical": None,
                    "prop_replied_posts_is_not_sociopolitical": None,
                    "prop_replied_posts_is_political_left": None,
                    "prop_replied_posts_is_political_right": None,
                    "prop_replied_posts_is_political_moderate": None,
                    "prop_replied_posts_is_political_unclear": None,
                    "prop_replied_posts_intergroup": None,
                    "prop_replied_posts_moral": None,
                    "prop_replied_posts_emotion": None,
                    "prop_replied_posts_other": None,
                },
            },
            "did_B": {
                "week_1": {
                    # Average of 0.4 and 0.6
                    "prop_liked_posts_toxic": 0.5,
                    # Average of 0.6 and 0.4
                    "prop_liked_posts_constructive": 0.5,
                    # Average of 0.7 and 0.3
                    "prop_liked_posts_sociopolitical": 0.5,
                    # Average of 0.3 and 0.7
                    "prop_liked_posts_is_not_sociopolitical": 0.5,
                    # Average of 0.2 and 0.1
                    "prop_liked_posts_is_political_left": 0.15,
                    # Average of 0.3 and 0.1
                    "prop_liked_posts_is_political_right": 0.2,
                    # Average of 0.0 and 0.1
                    "prop_liked_posts_is_political_moderate": 0.05,
                    # Average of 0.5 and 0.7
                    "prop_liked_posts_is_political_unclear": 0.6,
                    # Average of 0.2 and 0.3
                    "prop_liked_posts_intergroup": 0.25,
                    # Average of 0.5 and 0.2
                    "prop_liked_posts_moral": 0.35,
                    # Average of 0.4 and 0.6
                    "prop_liked_posts_emotion": 0.5,
                    # Average of 0.1 and 0.4
                    "prop_liked_posts_other": 0.25,
                    # Average of 0.2 and 0.3
                    "prop_posted_posts_toxic": 0.25,
                    # Average of 0.8 and 0.7
                    "prop_posted_posts_constructive": 0.75,
                    # Average of 0.6 and 0.4
                    "prop_posted_posts_sociopolitical": 0.5,
                    # Average of 0.4 and 0.6
                    "prop_posted_posts_is_not_sociopolitical": 0.5,
                    # Average of 0.3 and 0.2
                    "prop_posted_posts_is_political_left": 0.25,
                    # Average of 0.1 and 0.0
                    "prop_posted_posts_is_political_right": 0.05,
                    # Average of 0.2 and 0.0
                    "prop_posted_posts_is_political_moderate": 0.1,
                    # Average of 0.4 and 0.8
                    "prop_posted_posts_is_political_unclear": 0.6,
                    # Average of 0.3 and 0.2
                    "prop_posted_posts_intergroup": 0.25,
                    # Average of 0.4 and 0.3
                    "prop_posted_posts_moral": 0.35,
                    # Average of 0.5 and 0.7
                    "prop_posted_posts_emotion": 0.6,
                    # Average of 0.2 and 0.3
                    "prop_posted_posts_other": 0.25,
                    # Average of 0.5 and None
                    "prop_reposted_posts_toxic": 0.5,
                    "prop_reposted_posts_constructive": 0.5,
                    "prop_reposted_posts_sociopolitical": 0.5,
                    "prop_reposted_posts_is_not_sociopolitical": 0.5,
                    "prop_reposted_posts_is_political_left": 0.0,
                    "prop_reposted_posts_is_political_right": 0.0,
                    "prop_reposted_posts_is_political_moderate": 0.5,
                    "prop_reposted_posts_is_political_unclear": 0.5,
                    "prop_reposted_posts_intergroup": 0.5,
                    "prop_reposted_posts_moral": 0.5,
                    "prop_reposted_posts_emotion": 0.5,
                    "prop_reposted_posts_other": 0.5,
                    # Average of 0.0 and 0.0
                    "prop_replied_posts_toxic": 0.0,
                    # Average of 1.0 and 1.0
                    "prop_replied_posts_constructive": 1.0,
                    # Average of 1.0 and 0.0
                    "prop_replied_posts_sociopolitical": 0.5,
                    # Average of 0.0 and 1.0
                    "prop_replied_posts_is_not_sociopolitical": 0.5,
                    # Average of 0.0 and 0.0
                    "prop_replied_posts_is_political_left": 0.0,
                    # Average of 0.0 and 0.0
                    "prop_replied_posts_is_political_right": 0.0,
                    # Average of 1.0 and 0.0
                    "prop_replied_posts_is_political_moderate": 0.5,
                    # Average of 0.0 and 1.0
                    "prop_replied_posts_is_political_unclear": 0.5,
                    # Average of 0.0 and 0.0
                    "prop_replied_posts_intergroup": 0.0,
                    # Average of 0.0 and 0.0
                    "prop_replied_posts_moral": 0.0,
                    # Average of 1.0 and 0.0
                    "prop_replied_posts_emotion": 0.5,
                    # Average of 0.0 and 0.0
                    "prop_replied_posts_other": 0.0,
                },  # no data for Week 2 or 3
            },
        }

        # Call the function
        result = agg.get_per_user_to_weekly_content_label_proportions(
            user_per_day_content_label_proportions=user_per_day_content_label_proportions,
            user_date_to_week_df=user_date_to_week_df,
        )

        # Verify the result structure
        assert set(result.keys()) == set(
            expected_result.keys()
        ), "User DIDs don't match"

        # Verify all results for all users and weeks
        for user_did in expected_result:
            assert user_did in result, f"User {user_did} missing from results"

            for week in expected_result[user_did]:
                assert (
                    week in result[user_did]
                ), f"Week {week} missing for user {user_did}"

                # Verify all metrics for this user and week
                for metric, expected_value in expected_result[user_did][week].items():
                    assert (
                        metric in result[user_did][week]
                    ), f"Metric {metric} missing for user {user_did} in {week}"

                    if expected_value is None:
                        assert (
                            result[user_did][week][metric] is None
                        ), f"Expected None for {metric} but got {result[user_did][week][metric]}"
                    else:
                        assert (
                            result[user_did][week][metric]
                            == pytest.approx(expected_value, abs=0.001)
                        ), f"Metric {metric} for user {user_did} in {week}: expected {expected_value}, got {result[user_did][week][metric]}"


class TestTransformPerUserToWeeklyContentLabelProportions:
    def test_transform_per_user_to_weekly_content_label_proportions(self):
        """
        Test that transform_per_user_to_weekly_content_label_proportions correctly transforms
        the weekly content label proportions and handles users with no data.

        This test:
        - Uses data from test_weekly_aggregation_comprehensive
        - Adds a new user (did_C) who has no data in the input
        - Verifies that the transformation correctly preserves all metrics
        - Checks that users with no data are handled properly
        """
        # Use the same data structure as in test_weekly_aggregation_comprehensive
        user_to_weekly_content_label_proportions = {
            "did_A": {
                "week_1": {
                    "prop_liked_posts_toxic": 0.5,
                    "prop_liked_posts_constructive": 0.5,
                    "prop_liked_posts_sociopolitical": 0.5,
                    "prop_liked_posts_is_not_sociopolitical": 0.5,
                    "prop_liked_posts_is_political_left": 0.111,
                    "prop_liked_posts_is_political_right": 0.111,
                    "prop_liked_posts_is_political_moderate": 0.333,
                    "prop_liked_posts_is_political_unclear": 0.444,
                    "prop_liked_posts_intergroup": 0.222,
                    "prop_liked_posts_moral": 0.222,
                    "prop_liked_posts_emotion": 0.667,
                    "prop_liked_posts_other": 0.111,
                    "prop_posted_posts_toxic": 0.25,
                    "prop_posted_posts_constructive": 0.75,
                    "prop_posted_posts_sociopolitical": 0.167,
                    "prop_posted_posts_is_not_sociopolitical": 0.833,
                    "prop_posted_posts_is_political_left": 0.0,
                    "prop_posted_posts_is_political_right": 0.0,
                    "prop_posted_posts_is_political_moderate": 0.5,
                    "prop_posted_posts_is_political_unclear": 0.5,
                    "prop_posted_posts_intergroup": 0.167,
                    "prop_posted_posts_moral": 0.167,
                    "prop_posted_posts_emotion": 0.167,
                    "prop_posted_posts_other": 0.5,
                    "prop_reposted_posts_toxic": 1.0,
                    "prop_reposted_posts_constructive": 0.0,
                    "prop_reposted_posts_sociopolitical": 1.0,
                    "prop_reposted_posts_is_not_sociopolitical": 0.0,
                    "prop_reposted_posts_is_political_left": 1.0,
                    "prop_reposted_posts_is_political_right": 0.0,
                    "prop_reposted_posts_is_political_moderate": 0.0,
                    "prop_reposted_posts_is_political_unclear": 0.0,
                    "prop_reposted_posts_intergroup": 1.0,
                    "prop_reposted_posts_moral": 1.0,
                    "prop_reposted_posts_emotion": 0.0,
                    "prop_reposted_posts_other": 0.0,
                    "prop_replied_posts_toxic": 0.5,
                    "prop_replied_posts_constructive": 0.5,
                    "prop_replied_posts_sociopolitical": 0.5,
                    "prop_replied_posts_is_not_sociopolitical": 0.5,
                    "prop_replied_posts_is_political_left": 0.0,
                    "prop_replied_posts_is_political_right": 0.0,
                    "prop_replied_posts_is_political_moderate": 0.0,
                    "prop_replied_posts_is_political_unclear": 1.0,
                    "prop_replied_posts_intergroup": 0.5,
                    "prop_replied_posts_moral": 0.5,
                    "prop_replied_posts_emotion": 0.5,
                    "prop_replied_posts_other": 0.0,
                },
                "week_3": {
                    "prop_liked_posts_toxic": 0.2,
                    "prop_liked_posts_constructive": 0.8,
                    "prop_liked_posts_sociopolitical": 0.6,
                    "prop_liked_posts_is_not_sociopolitical": 0.4,
                    "prop_liked_posts_is_political_left": 0.5,
                    "prop_liked_posts_is_political_right": 0.0,
                    "prop_liked_posts_is_political_moderate": 0.0,
                    "prop_liked_posts_is_political_unclear": 0.5,
                    "prop_liked_posts_intergroup": 0.8,
                    "prop_liked_posts_moral": 0.6,
                    "prop_liked_posts_emotion": 0.4,
                    "prop_liked_posts_other": 0.2,
                    "prop_posted_posts_toxic": 0.75,
                    "prop_posted_posts_constructive": 0.25,
                    "prop_posted_posts_sociopolitical": 1.0,
                    "prop_posted_posts_is_not_sociopolitical": 0.0,
                    "prop_posted_posts_is_political_left": 1.0,
                    "prop_posted_posts_is_political_right": 0.0,
                    "prop_posted_posts_is_political_moderate": 0.0,
                    "prop_posted_posts_is_political_unclear": 0.0,
                    "prop_posted_posts_intergroup": 0.75,
                    "prop_posted_posts_moral": 0.75,
                    "prop_posted_posts_emotion": 0.25,
                    "prop_posted_posts_other": 0.0,
                    "prop_reposted_posts_toxic": None,
                    "prop_reposted_posts_constructive": None,
                    "prop_reposted_posts_sociopolitical": None,
                    "prop_reposted_posts_is_not_sociopolitical": None,
                    "prop_reposted_posts_is_political_left": None,
                    "prop_reposted_posts_is_political_right": None,
                    "prop_reposted_posts_is_political_moderate": None,
                    "prop_reposted_posts_is_political_unclear": None,
                    "prop_reposted_posts_intergroup": None,
                    "prop_reposted_posts_moral": None,
                    "prop_reposted_posts_emotion": None,
                    "prop_reposted_posts_other": None,
                    "prop_replied_posts_toxic": None,
                    "prop_replied_posts_constructive": None,
                    "prop_replied_posts_sociopolitical": None,
                    "prop_replied_posts_is_not_sociopolitical": None,
                    "prop_replied_posts_is_political_left": None,
                    "prop_replied_posts_is_political_right": None,
                    "prop_replied_posts_is_political_moderate": None,
                    "prop_replied_posts_is_political_unclear": None,
                    "prop_replied_posts_intergroup": None,
                    "prop_replied_posts_moral": None,
                    "prop_replied_posts_emotion": None,
                    "prop_replied_posts_other": None,
                },
            },
            "did_B": {
                "week_1": {
                    "prop_liked_posts_toxic": 0.5,
                    "prop_liked_posts_constructive": 0.5,
                    "prop_liked_posts_sociopolitical": 0.5,
                    "prop_liked_posts_is_not_sociopolitical": 0.5,
                    "prop_liked_posts_is_political_left": 0.15,
                    "prop_liked_posts_is_political_right": 0.2,
                    "prop_liked_posts_is_political_moderate": 0.05,
                    "prop_liked_posts_is_political_unclear": 0.6,
                    "prop_liked_posts_intergroup": 0.25,
                    "prop_liked_posts_moral": 0.35,
                    "prop_liked_posts_emotion": 0.5,
                    "prop_liked_posts_other": 0.25,
                    "prop_posted_posts_toxic": 0.25,
                    "prop_posted_posts_constructive": 0.75,
                    "prop_posted_posts_sociopolitical": 0.5,
                    "prop_posted_posts_is_not_sociopolitical": 0.5,
                    "prop_posted_posts_is_political_left": 0.25,
                    "prop_posted_posts_is_political_right": 0.05,
                    "prop_posted_posts_is_political_moderate": 0.1,
                    "prop_posted_posts_is_political_unclear": 0.6,
                    "prop_posted_posts_intergroup": 0.25,
                    "prop_posted_posts_moral": 0.35,
                    "prop_posted_posts_emotion": 0.6,
                    "prop_posted_posts_other": 0.25,
                    "prop_reposted_posts_toxic": 0.5,
                    "prop_reposted_posts_constructive": 0.5,
                    "prop_reposted_posts_sociopolitical": 0.5,
                    "prop_reposted_posts_is_not_sociopolitical": 0.5,
                    "prop_reposted_posts_is_political_left": 0.0,
                    "prop_reposted_posts_is_political_right": 0.0,
                    "prop_reposted_posts_is_political_moderate": 0.5,
                    "prop_reposted_posts_is_political_unclear": 0.5,
                    "prop_reposted_posts_intergroup": 0.5,
                    "prop_reposted_posts_moral": 0.5,
                    "prop_reposted_posts_emotion": 0.5,
                    "prop_reposted_posts_other": 0.5,
                    "prop_replied_posts_toxic": 0.0,
                    "prop_replied_posts_constructive": 1.0,
                    "prop_replied_posts_sociopolitical": 0.5,
                    "prop_replied_posts_is_not_sociopolitical": 0.5,
                    "prop_replied_posts_is_political_left": 0.0,
                    "prop_replied_posts_is_political_right": 0.0,
                    "prop_replied_posts_is_political_moderate": 0.5,
                    "prop_replied_posts_is_political_unclear": 0.5,
                    "prop_replied_posts_intergroup": 0.0,
                    "prop_replied_posts_moral": 0.0,
                    "prop_replied_posts_emotion": 0.5,
                    "prop_replied_posts_other": 0.0,
                },
            },
        }

        # Create a user_date_to_week_df that includes did_C
        # Create dates from 2024-10-01 to 2024-10-18
        dates = [f"2024-10-{str(day).zfill(2)}" for day in range(1, 19)]

        # Assign weeks (6 days per week)
        weeks = ["week_1"] * 6 + ["week_2"] * 6 + ["week_3"] * 6

        # Create a list with each date repeated for all 3 users
        all_dates = dates * 3
        all_weeks = weeks * 3
        all_users = ["did_A"] * 18 + ["did_B"] * 18 + ["did_C"] * 18

        user_date_to_week_df = pd.DataFrame(
            {"bluesky_user_did": all_users, "date": all_dates, "week": all_weeks}
        )

        # Define expected result including did_C with empty data
        # The expected result should be a pandas DataFrame with bluesky_handle, condition, week, and metrics
        expected_result = pd.DataFrame(
            [
                # User A, Week 1
                {
                    "handle": "handle_A",
                    "condition": "condition_A",
                    "week": "week_1",
                    **user_to_weekly_content_label_proportions["did_A"]["week_1"],
                },
                # User A, Week 2 - should be None values.
                {
                    "handle": "handle_A",
                    "condition": "condition_A",
                    "week": "week_2",
                    **{label: None for label in default_content_engagement_columns},
                },
                # User A, Week 3
                {
                    "handle": "handle_A",
                    "condition": "condition_A",
                    "week": "week_3",
                    **user_to_weekly_content_label_proportions["did_A"]["week_3"],
                },
                # User B, Week 1
                {
                    "handle": "handle_B",
                    "condition": "condition_B",
                    "week": "week_1",
                    **user_to_weekly_content_label_proportions["did_B"]["week_1"],
                },
                # User B, Week 2 - should be None values.
                {
                    "handle": "handle_B",
                    "condition": "condition_B",
                    "week": "week_2",
                    **{label: None for label in default_content_engagement_columns},
                },
                # User B, Week 3 - should be None values.
                {
                    "handle": "handle_B",
                    "condition": "condition_B",
                    "week": "week_3",
                    **{label: None for label in default_content_engagement_columns},
                },
                # User C, Week 1 (with None values since did_C has no data)
                {
                    "handle": "handle_C",
                    "condition": "condition_C",
                    "week": "week_1",
                    **{label: None for label in default_content_engagement_columns},
                },
                # User C, Week 2 - should be None values.
                {
                    "handle": "handle_C",
                    "condition": "condition_C",
                    "week": "week_2",
                    **{label: None for label in default_content_engagement_columns},
                },
                # User C, Week 3 - should be None values.
                {
                    "handle": "handle_C",
                    "condition": "condition_C",
                    "week": "week_3",
                    **{label: None for label in default_content_engagement_columns},
                },
            ]
        )

        users = [
            {
                "bluesky_user_did": "did_A",
                "bluesky_handle": "handle_A",
                "condition": "condition_A",
            },
            {
                "bluesky_user_did": "did_B",
                "bluesky_handle": "handle_B",
                "condition": "condition_B",
            },
            {
                "bluesky_user_did": "did_C",
                "bluesky_handle": "handle_C",
                "condition": "condition_C",
            },
        ]

        # Call the function
        result: pd.DataFrame = agg.transform_per_user_to_weekly_content_label_proportions(
            user_to_weekly_content_label_proportions=user_to_weekly_content_label_proportions,
            user_date_to_week_df=user_date_to_week_df,
            users=users,
        )

        # Verify the result structure
        assert set(result.columns) == set(
            expected_result.columns
        ), "DataFrame columns don't match"

        # Sort both dataframes to ensure consistent comparison
        result_sorted = result.sort_values(by=["handle", "week"]).reset_index(drop=True)
        expected_sorted = expected_result.sort_values(
            by=["handle", "week"]
        ).reset_index(drop=True)

        # Check that we have the right number of rows
        assert len(result) == len(
            expected_result
        ), f"Expected {len(expected_result)} rows, got {len(result)}"

        # Verify all rows match the expected values
        for idx, expected_row in expected_sorted.iterrows():
            result_row = result_sorted.iloc[idx]

            # Check handle, condition and week match
            assert (
                result_row["handle"] == expected_row["handle"]
            ), f"Handle mismatch at row {idx}"
            assert (
                result_row["condition"] == expected_row["condition"]
            ), f"Condition mismatch at row {idx}"
            assert (
                result_row["week"] == expected_row["week"]
            ), f"Week mismatch at row {idx}"

            # Check all metrics
            for metric in default_content_engagement_columns:
                expected_value = expected_row[metric]
                actual_value = result_row[metric]

                if pd.isna(expected_value):
                    assert pd.isna(
                        actual_value
                    ), f"Expected None for {metric} but got {actual_value} for {result_row['handle']} in {result_row['week']}"
                else:
                    assert (
                        actual_value == pytest.approx(expected_value, abs=0.001)
                    ), f"Metric {metric} for {result_row['handle']} in {result_row['week']}: expected {expected_value}, got {actual_value}"
