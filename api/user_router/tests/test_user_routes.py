"""Tests for user API routes.

This module tests the FastAPI endpoints for user profile access and scoring.
"""

import pytest
from unittest.mock import patch, MagicMock
from fastapi.testclient import TestClient

from api.main import app
from services.participant_data.models import UserProfileResponse


client = TestClient(app)


class TestUserProfileEndpoint:
    """Test cases for the user profile endpoint."""

    @patch('api.user_router.routes.get_user_profile_response')
    def test_get_user_profile_success(self, mock_get_profile):
        """Test successful user profile retrieval."""
        # Mock user profile response
        mock_profile = UserProfileResponse(
            study_user_id='test_id',
            bluesky_handle='testuser.bsky.social',
            bluesky_user_did='did:plc:test123',
            condition='engagement',
            score=10,
            is_study_user=True,
            created_timestamp='2025-01-01T00:00:00Z'
        )
        mock_get_profile.return_value = mock_profile
        
        response = client.get("/user/testuser.bsky.social")
        
        assert response.status_code == 200
        data = response.json()
        assert data['bluesky_handle'] == 'testuser.bsky.social'
        assert data['score'] == 10
        assert data['condition'] == 'engagement'

    @patch('api.user_router.routes.get_user_profile_response')
    def test_get_user_profile_with_at_symbol(self, mock_get_profile):
        """Test user profile retrieval with @ symbol in handle."""
        # Mock user profile response
        mock_profile = UserProfileResponse(
            study_user_id='test_id',
            bluesky_handle='testuser.bsky.social',
            bluesky_user_did='did:plc:test123',
            condition='engagement',
            score=5,
            is_study_user=True,
            created_timestamp='2025-01-01T00:00:00Z'
        )
        mock_get_profile.return_value = mock_profile
        
        response = client.get("/user/@testuser.bsky.social")
        
        assert response.status_code == 200
        # Verify that the @ was stripped when calling the service
        mock_get_profile.assert_called_once_with('testuser.bsky.social')

    @patch('api.user_router.routes.get_user_profile_response')
    def test_get_user_profile_not_found(self, mock_get_profile):
        """Test user profile retrieval when user doesn't exist."""
        mock_get_profile.return_value = None
        
        response = client.get("/user/nonexistent.bsky.social")
        
        assert response.status_code == 404
        data = response.json()
        assert "not found" in data['detail'].lower()

    @patch('api.user_router.routes.get_user_profile_response')
    def test_get_user_profile_server_error(self, mock_get_profile):
        """Test user profile retrieval with server error."""
        mock_get_profile.side_effect = Exception("Database connection error")
        
        response = client.get("/user/testuser.bsky.social")
        
        assert response.status_code == 500
        data = response.json()
        assert "Internal server error" in data['detail']


class TestUserScoreEndpoint:
    """Test cases for the user score endpoint."""

    @patch('api.user_router.routes.get_user_profile_response')
    def test_get_user_score_success(self, mock_get_profile):
        """Test successful user score retrieval."""
        # Mock user profile response
        mock_profile = UserProfileResponse(
            study_user_id='test_id',
            bluesky_handle='testuser.bsky.social',
            bluesky_user_did='did:plc:test123',
            condition='engagement',
            score=15,
            is_study_user=True,
            created_timestamp='2025-01-01T00:00:00Z'
        )
        mock_get_profile.return_value = mock_profile
        
        response = client.get("/user/testuser.bsky.social/score")
        
        assert response.status_code == 200
        data = response.json()
        assert data['bluesky_handle'] == 'testuser.bsky.social'
        assert data['score'] == 15

    @patch('api.user_router.routes.get_user_profile_response')
    def test_get_user_score_with_at_symbol(self, mock_get_profile):
        """Test user score retrieval with @ symbol in handle."""
        # Mock user profile response
        mock_profile = UserProfileResponse(
            study_user_id='test_id',
            bluesky_handle='testuser.bsky.social',
            bluesky_user_did='did:plc:test123',
            condition='engagement',
            score=8,
            is_study_user=True,
            created_timestamp='2025-01-01T00:00:00Z'
        )
        mock_get_profile.return_value = mock_profile
        
        response = client.get("/user/@testuser.bsky.social/score")
        
        assert response.status_code == 200
        data = response.json()
        assert data['score'] == 8
        # Verify that the @ was stripped when calling the service
        mock_get_profile.assert_called_once_with('testuser.bsky.social')

    @patch('api.user_router.routes.get_user_profile_response')
    def test_get_user_score_not_found(self, mock_get_profile):
        """Test user score retrieval when user doesn't exist."""
        mock_get_profile.return_value = None
        
        response = client.get("/user/nonexistent.bsky.social/score")
        
        assert response.status_code == 404
        data = response.json()
        assert "not found" in data['detail'].lower()

    @patch('api.user_router.routes.get_user_profile_response')
    def test_get_user_score_server_error(self, mock_get_profile):
        """Test user score retrieval with server error."""
        mock_get_profile.side_effect = Exception("Database connection error")
        
        response = client.get("/user/testuser.bsky.social/score")
        
        assert response.status_code == 500
        data = response.json()
        assert "Internal server error" in data['detail']


class TestAPIIntegration:
    """Integration tests for the user API."""

    def test_root_endpoint(self):
        """Test the root API endpoint."""
        response = client.get("/")
        
        assert response.status_code == 200
        data = response.json()
        assert "Bluesky Research Platform API" in data['message']
        assert 'endpoints' in data

    def test_health_check_endpoint(self):
        """Test the health check endpoint."""
        response = client.get("/health")
        
        assert response.status_code == 200
        data = response.json()
        assert data['status'] == 'healthy'

    def test_api_documentation_accessible(self):
        """Test that API documentation is accessible."""
        response = client.get("/docs")
        
        # Should return some form of documentation (HTML or redirect)
        assert response.status_code in [200, 307]  # 307 for redirect


class TestErrorHandling:
    """Test cases for error handling in the API."""

    def test_invalid_endpoint(self):
        """Test accessing an invalid endpoint."""
        response = client.get("/user/")
        
        # Should return 404 or 422 (validation error)
        assert response.status_code in [404, 422]

    def test_malformed_handle(self):
        """Test accessing user endpoint with malformed handle."""
        # Even malformed handles should be processed and return 404 if not found
        response = client.get("/user/invalid@@@handle")
        
        # The API should handle this gracefully
        assert response.status_code in [404, 422]