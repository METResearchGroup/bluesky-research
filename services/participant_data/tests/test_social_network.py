"""Tests for social_network.py.

This test suite verifies the functionality of social network processing functions:
- build_user_social_network_map: Building user social network mappings
- load_user_social_network_map: Loading and processing social network data
- Error handling and edge cases

The tests use mocks to isolate the data processing logic from external dependencies.
"""

import pytest
import pandas as pd
from unittest.mock import patch, MagicMock

from services.participant_data.social_network import (
    build_user_social_network_map,
    load_user_social_network_map,
)
from services.participant_data.models import SocialNetworkRelationshipModel


class TestBuildUserSocialNetworkMap:
    """Tests for build_user_social_network_map function."""

    def test_build_user_social_network_map_follower_relationship(self):
        """Test building map with follower relationship type."""
        # Arrange
        social_network_dicts = [
            SocialNetworkRelationshipModel(
                relationship_to_study_user="follower",
                follow_did="did:plc:study_user",
                follower_did="did:plc:follower_user",
            )
        ]

        # Act
        result = build_user_social_network_map(social_network_dicts)

        # Assert
        assert isinstance(result, dict)
        assert "did:plc:study_user" in result
        assert "did:plc:follower_user" in result["did:plc:study_user"]
        assert len(result["did:plc:study_user"]) == 1

    def test_build_user_social_network_map_follow_relationship(self):
        """Test building map with follow relationship type."""
        # Arrange
        social_network_dicts = [
            SocialNetworkRelationshipModel(
                relationship_to_study_user="follow",
                follow_did="did:plc:followee_user",
                follower_did="did:plc:study_user",
            )
        ]

        # Act
        result = build_user_social_network_map(social_network_dicts)

        # Assert
        assert isinstance(result, dict)
        assert "did:plc:study_user" in result
        assert "did:plc:followee_user" in result["did:plc:study_user"]
        assert len(result["did:plc:study_user"]) == 1

    def test_build_user_social_network_map_multiple_connections(self):
        """Test building map with multiple connections for one user."""
        # Arrange
        social_network_dicts = [
            SocialNetworkRelationshipModel(
                relationship_to_study_user="follower",
                follow_did="did:plc:study_user",
                follower_did="did:plc:follower1",
            ),
            SocialNetworkRelationshipModel(
                relationship_to_study_user="follow",
                follow_did="did:plc:followee1",
                follower_did="did:plc:study_user",
            ),
            SocialNetworkRelationshipModel(
                relationship_to_study_user="follow",
                follow_did="did:plc:followee2",
                follower_did="did:plc:study_user",
            ),
        ]

        # Act
        result = build_user_social_network_map(social_network_dicts)

        # Assert
        assert isinstance(result, dict)
        assert "did:plc:study_user" in result
        assert len(result["did:plc:study_user"]) == 3
        assert "did:plc:follower1" in result["did:plc:study_user"]
        assert "did:plc:followee1" in result["did:plc:study_user"]
        assert "did:plc:followee2" in result["did:plc:study_user"]

    def test_build_user_social_network_map_multiple_users(self):
        """Test building map with multiple study users."""
        # Arrange
        social_network_dicts = [
            SocialNetworkRelationshipModel(
                relationship_to_study_user="follower",
                follow_did="did:plc:user1",
                follower_did="did:plc:connection1",
            ),
            SocialNetworkRelationshipModel(
                relationship_to_study_user="follow",
                follow_did="did:plc:connection2",
                follower_did="did:plc:user2",
            ),
        ]

        # Act
        result = build_user_social_network_map(social_network_dicts)

        # Assert
        assert isinstance(result, dict)
        assert "did:plc:user1" in result
        assert "did:plc:user2" in result
        assert len(result["did:plc:user1"]) == 1
        assert len(result["did:plc:user2"]) == 1

    def test_build_user_social_network_map_empty_input(self):
        """Test building map with empty input."""
        # Arrange
        social_network_dicts = []

        # Act
        result = build_user_social_network_map(social_network_dicts)

        # Assert
        assert isinstance(result, dict)
        assert len(result) == 0

    def test_build_user_social_network_map_returns_regular_dict(self):
        """Test that function returns regular dict, not defaultdict."""
        # Arrange
        social_network_dicts = [
            SocialNetworkRelationshipModel(
                relationship_to_study_user="follower",
                follow_did="did:plc:study_user",
                follower_did="did:plc:follower_user",
            )
        ]

        # Act
        result = build_user_social_network_map(social_network_dicts)

        # Assert
        assert isinstance(result, dict)
        assert type(result).__name__ == "dict"  # Not defaultdict


class TestLoadUserSocialNetworkMap:
    """Tests for load_user_social_network_map function."""

    @patch("services.participant_data.social_network.load_data_from_local_storage")
    @patch("services.participant_data.social_network.parse_converted_pandas_dicts")
    def test_load_user_social_network_map_success(
        self, mock_parse_dicts, mock_load_data
    ):
        """Test successful loading of user social network map."""
        # Arrange
        mock_df = pd.DataFrame({
            "relationship_to_study_user": ["follower", "follow"],
            "follow_did": ["did:plc:user1", "did:plc:followee"],
            "follower_did": ["did:plc:follower", "did:plc:user1"],
        })
        mock_load_data.return_value = mock_df

        mock_dicts = [
            {
                "relationship_to_study_user": "follower",
                "follow_did": "did:plc:user1",
                "follower_did": "did:plc:follower",
            },
            {
                "relationship_to_study_user": "follow",
                "follow_did": "did:plc:followee",
                "follower_did": "did:plc:user1",
            },
        ]
        mock_parse_dicts.return_value = mock_dicts

        # Act
        result = load_user_social_network_map()

        # Assert
        assert isinstance(result, dict)
        mock_load_data.assert_called_once_with(
            service="scraped_user_social_network",
            latest_timestamp=None,
            use_all_data=True,
            validate_pq_files=True,
        )

    @patch("services.participant_data.social_network.load_data_from_local_storage")
    @patch("services.participant_data.social_network.parse_converted_pandas_dicts")
    def test_load_user_social_network_map_handles_empty_data(
        self, mock_parse_dicts, mock_load_data
    ):
        """Test loading with empty data."""
        # Arrange
        mock_load_data.return_value = pd.DataFrame()
        mock_parse_dicts.return_value = []

        # Act
        result = load_user_social_network_map()

        # Assert
        assert isinstance(result, dict)
        assert len(result) == 0

