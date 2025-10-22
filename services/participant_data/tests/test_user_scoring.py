"""Tests for user scoring functionality.

This module tests the user score calculation logic, API endpoints, and
database operations related to user scoring.
"""

import pytest
from unittest.mock import patch, MagicMock

from services.participant_data.helper import (
    calculate_user_score,
    get_user_by_handle,
    update_user_score,
    get_user_profile_response
)
from services.participant_data.models import UserToBlueskyProfileModel, UserProfileResponse


class TestUserScoreCalculation:
    """Test cases for user score calculation logic."""

    @patch('services.participant_data.helper.PostsWrittenByStudyUsers')
    def test_calculate_user_score_with_posts(self, mock_posts):
        """Test score calculation for user with posts."""
        # Mock post count query
        mock_query = MagicMock()
        mock_query.count.return_value = 5
        mock_posts.select.return_value.where.return_value = mock_query
        
        user_did = "did:plc:test123"
        score = calculate_user_score(user_did)
        
        assert score == 5
        mock_posts.select.assert_called_once()

    @patch('services.participant_data.helper.PostsWrittenByStudyUsers')
    def test_calculate_user_score_no_posts(self, mock_posts):
        """Test score calculation for user with no posts."""
        # Mock post count query returning 0
        mock_query = MagicMock()
        mock_query.count.return_value = 0
        mock_posts.select.return_value.where.return_value = mock_query
        
        user_did = "did:plc:test123"
        score = calculate_user_score(user_did)
        
        assert score == 0

    @patch('services.participant_data.helper.PostsWrittenByStudyUsers')
    def test_calculate_user_score_database_error(self, mock_posts):
        """Test score calculation with database error."""
        # Mock database error
        mock_posts.select.side_effect = Exception("Database connection error")
        
        user_did = "did:plc:test123"
        score = calculate_user_score(user_did)
        
        # Should return 0 on error
        assert score == 0


class TestUserByHandle:
    """Test cases for getting user by handle with score calculation."""

    @patch('services.participant_data.helper.table')
    @patch('services.participant_data.helper.calculate_user_score')
    @patch('services.participant_data.helper.update_user_score')
    def test_get_user_by_handle_success(self, mock_update_score, mock_calc_score, mock_table):
        """Test successful user retrieval by handle."""
        # Mock DynamoDB response
        mock_table.scan.return_value = {
            'Items': [{
                'study_user_id': 'test_id',
                'bluesky_handle': 'testuser.bsky.social',
                'bluesky_user_did': 'did:plc:test123',
                'condition': 'engagement',
                'is_study_user': True,
                'created_timestamp': '2025-01-01T00:00:00Z',
                'score': 3
            }]
        }
        
        # Mock score calculation
        mock_calc_score.return_value = 5
        
        user = get_user_by_handle('testuser.bsky.social')
        
        assert user is not None
        assert user.bluesky_handle == 'testuser.bsky.social'
        assert user.score == 5
        
        # Should update score in database since it changed
        mock_update_score.assert_called_once_with('did:plc:test123', 5)

    @patch('services.participant_data.helper.table')
    def test_get_user_by_handle_not_found(self, mock_table):
        """Test user retrieval when user doesn't exist."""
        # Mock empty DynamoDB response
        mock_table.scan.return_value = {'Items': []}
        
        user = get_user_by_handle('nonexistent.bsky.social')
        
        assert user is None

    @patch('services.participant_data.helper.table')
    def test_get_user_by_handle_database_error(self, mock_table):
        """Test user retrieval with database error."""
        from botocore.exceptions import ClientError
        
        # Mock DynamoDB error
        error = ClientError({'Error': {'Code': 'TestError'}}, 'scan')
        mock_table.scan.side_effect = error
        
        user = get_user_by_handle('testuser.bsky.social')
        
        assert user is None


class TestUserScoreUpdate:
    """Test cases for updating user scores in database."""

    @patch('services.participant_data.helper.table')
    def test_update_user_score_success(self, mock_table):
        """Test successful score update."""
        mock_table.update_item.return_value = {}
        
        # Should not raise exception
        update_user_score('did:plc:test123', 10)
        
        mock_table.update_item.assert_called_once_with(
            Key={'bluesky_user_did': 'did:plc:test123'},
            UpdateExpression='SET score = :score',
            ExpressionAttributeValues={':score': 10}
        )

    @patch('services.participant_data.helper.table')
    def test_update_user_score_database_error(self, mock_table):
        """Test score update with database error."""
        from botocore.exceptions import ClientError
        
        # Mock DynamoDB error
        error = ClientError({'Error': {'Code': 'TestError'}}, 'update_item')
        mock_table.update_item.side_effect = error
        
        with pytest.raises(ClientError):
            update_user_score('did:plc:test123', 10)


class TestUserProfileResponse:
    """Test cases for user profile API response generation."""

    @patch('services.participant_data.helper.get_user_by_handle')
    def test_get_user_profile_response_success(self, mock_get_user):
        """Test successful user profile response generation."""
        # Mock user data
        mock_user = UserToBlueskyProfileModel(
            study_user_id='test_id',
            bluesky_handle='testuser.bsky.social',
            bluesky_user_did='did:plc:test123',
            condition='engagement',
            is_study_user=True,
            created_timestamp='2025-01-01T00:00:00Z',
            score=7
        )
        mock_get_user.return_value = mock_user
        
        response = get_user_profile_response('testuser.bsky.social')
        
        assert response is not None
        assert isinstance(response, UserProfileResponse)
        assert response.bluesky_handle == 'testuser.bsky.social'
        assert response.score == 7
        assert response.condition == 'engagement'

    @patch('services.participant_data.helper.get_user_by_handle')
    def test_get_user_profile_response_user_not_found(self, mock_get_user):
        """Test user profile response when user doesn't exist."""
        mock_get_user.return_value = None
        
        response = get_user_profile_response('nonexistent.bsky.social')
        
        assert response is None


class TestUserScoringIntegration:
    """Integration tests for user scoring functionality."""

    def test_user_model_score_default(self):
        """Test that user model has default score of 0."""
        user = UserToBlueskyProfileModel(
            study_user_id='test_id',
            bluesky_handle='testuser.bsky.social',
            bluesky_user_did='did:plc:test123',
            condition='engagement',
            is_study_user=True,
            created_timestamp='2025-01-01T00:00:00Z'
        )
        
        assert user.score == 0

    def test_user_profile_response_model(self):
        """Test UserProfileResponse model creation."""
        response = UserProfileResponse(
            study_user_id='test_id',
            bluesky_handle='testuser.bsky.social',
            bluesky_user_did='did:plc:test123',
            condition='engagement',
            score=15,
            is_study_user=True,
            created_timestamp='2025-01-01T00:00:00Z'
        )
        
        assert response.score == 15
        assert response.bluesky_handle == 'testuser.bsky.social'