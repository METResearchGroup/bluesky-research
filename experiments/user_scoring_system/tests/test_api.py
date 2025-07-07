"""Tests for API endpoints in the experimental user scoring system.

This module tests all FastAPI endpoints for user profiles, scores,
and CRUD operations.
"""

import pytest
import tempfile
import os
from datetime import datetime
from unittest.mock import patch, MagicMock

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from fastapi.testclient import TestClient
from api_server import app
from database import Database


class TestUserAPI:
    """Test cases for user-related API endpoints."""
    
    def setup_method(self):
        """Set up test environment for each test method."""
        # Create temporary database
        self.temp_db = tempfile.NamedTemporaryFile(delete=False)
        self.temp_db.close()
        
        # Mock the database dependency
        self.test_db = Database(self.temp_db.name)
        
        # Override the database dependency in the FastAPI app
        def get_test_database():
            return self.test_db
        
        app.dependency_overrides[app.dependency_overrides.get("get_database", lambda: None)] = get_test_database
        
        # Create test client
        self.client = TestClient(app)
    
    def teardown_method(self):
        """Clean up test environment after each test method."""
        # Remove temporary database file
        os.unlink(self.temp_db.name)
        
        # Clear dependency overrides
        app.dependency_overrides.clear()
    
    def test_root_endpoint(self):
        """Test the root endpoint returns API information."""
        response = self.client.get("/")
        
        assert response.status_code == 200
        data = response.json()
        
        assert "message" in data
        assert "version" in data
        assert "endpoints" in data
        assert data["message"] == "User Scoring System API"
        assert data["version"] == "1.0.0"
    
    def test_health_check(self):
        """Test the health check endpoint."""
        response = self.client.get("/health")
        
        assert response.status_code == 200
        data = response.json()
        
        assert data["status"] == "healthy"
        assert "timestamp" in data
        assert data["service"] == "user-scoring-system"
    
    def test_get_user_profile_success(self):
        """Test successful user profile retrieval."""
        # Create test user
        user = self.test_db.create_user(
            handle="test.user",
            display_name="Test User",
            bio="Test bio"
        )
        
        # Create posts to test score calculation
        self.test_db.create_post(user.user_id, "Test post 1")
        self.test_db.create_post(user.user_id, "Test post 2")
        
        # Make API request
        response = self.client.get("/user/test.user")
        
        assert response.status_code == 200
        data = response.json()
        
        assert data["handle"] == "test.user"
        assert data["display_name"] == "Test User"
        assert data["bio"] == "Test bio"
        assert data["score"] == 2
        assert data["post_count"] == 2
        assert "user_id" in data
        assert "created_at" in data
        assert "updated_at" in data
    
    def test_get_user_profile_not_found(self):
        """Test user profile retrieval for nonexistent user."""
        response = self.client.get("/user/nonexistent.user")
        
        assert response.status_code == 404
        data = response.json()
        
        assert "detail" in data
        assert "nonexistent.user" in data["detail"]
    
    def test_get_user_score_success(self):
        """Test successful user score retrieval."""
        # Create test user
        user = self.test_db.create_user(
            handle="score.user",
            display_name="Score User"
        )
        
        # Create posts
        self.test_db.create_post(user.user_id, "Score post 1")
        self.test_db.create_post(user.user_id, "Score post 2")
        self.test_db.create_post(user.user_id, "Score post 3")
        
        # Make API request
        response = self.client.get("/user/score.user/score")
        
        assert response.status_code == 200
        data = response.json()
        
        assert data["handle"] == "score.user"
        assert data["score"] == 3
        assert data["post_count"] == 3
        assert "user_id" in data
        assert "last_calculated" in data
    
    def test_get_user_score_not_found(self):
        """Test user score retrieval for nonexistent user."""
        response = self.client.get("/user/nonexistent.user/score")
        
        assert response.status_code == 404
        data = response.json()
        
        assert "detail" in data
        assert "nonexistent.user" in data["detail"]
    
    def test_recalculate_user_score(self):
        """Test user score recalculation endpoint."""
        # Create test user
        user = self.test_db.create_user(
            handle="recalc.user",
            display_name="Recalc User"
        )
        
        # Create posts
        self.test_db.create_post(user.user_id, "Recalc post 1")
        self.test_db.create_post(user.user_id, "Recalc post 2")
        
        # Make API request
        response = self.client.post("/user/recalc.user/recalculate")
        
        assert response.status_code == 200
        data = response.json()
        
        assert data["handle"] == "recalc.user"
        assert data["score"] == 2
        assert data["post_count"] == 2
    
    def test_recalculate_user_score_not_found(self):
        """Test score recalculation for nonexistent user."""
        response = self.client.post("/user/nonexistent.user/recalculate")
        
        assert response.status_code == 404
        data = response.json()
        
        assert "detail" in data
        assert "nonexistent.user" in data["detail"]
    
    def test_get_user_posts(self):
        """Test retrieving user posts."""
        # Create test user
        user = self.test_db.create_user(
            handle="posts.user",
            display_name="Posts User"
        )
        
        # Create posts
        post1 = self.test_db.create_post(user.user_id, "First post")
        post2 = self.test_db.create_post(user.user_id, "Second post")
        
        # Make API request
        response = self.client.get("/user/posts.user/posts")
        
        assert response.status_code == 200
        data = response.json()
        
        assert len(data) == 2
        assert data[0]["content"] == "Second post"  # Most recent first
        assert data[1]["content"] == "First post"
        assert data[0]["user_id"] == user.user_id
        assert data[1]["user_id"] == user.user_id
    
    def test_get_user_posts_not_found(self):
        """Test retrieving posts for nonexistent user."""
        response = self.client.get("/user/nonexistent.user/posts")
        
        assert response.status_code == 404
        data = response.json()
        
        assert "detail" in data
        assert "nonexistent.user" in data["detail"]
    
    def test_create_user_success(self):
        """Test successful user creation."""
        user_data = {
            "handle": "new.user",
            "display_name": "New User",
            "bio": "I'm a new user",
            "avatar_url": "https://example.com/avatar.jpg"
        }
        
        response = self.client.post("/users", json=user_data)
        
        assert response.status_code == 200
        data = response.json()
        
        assert data["handle"] == "new.user"
        assert data["display_name"] == "New User"
        assert data["bio"] == "I'm a new user"
        assert data["avatar_url"] == "https://example.com/avatar.jpg"
        assert data["score"] == 0
        assert "user_id" in data
        assert "created_at" in data
        assert "updated_at" in data
    
    def test_create_user_minimal_data(self):
        """Test user creation with minimal required data."""
        user_data = {
            "handle": "minimal.user",
            "display_name": "Minimal User"
        }
        
        response = self.client.post("/users", json=user_data)
        
        assert response.status_code == 200
        data = response.json()
        
        assert data["handle"] == "minimal.user"
        assert data["display_name"] == "Minimal User"
        assert data["bio"] is None
        assert data["avatar_url"] is None
        assert data["score"] == 0
    
    def test_create_user_duplicate_handle(self):
        """Test user creation with duplicate handle."""
        # Create first user
        user_data = {
            "handle": "duplicate.user",
            "display_name": "First User"
        }
        response = self.client.post("/users", json=user_data)
        assert response.status_code == 200
        
        # Try to create second user with same handle
        user_data2 = {
            "handle": "duplicate.user",
            "display_name": "Second User"
        }
        response = self.client.post("/users", json=user_data2)
        
        assert response.status_code == 400
        data = response.json()
        
        assert "detail" in data
        assert "duplicate.user" in data["detail"]
    
    def test_create_user_missing_fields(self):
        """Test user creation with missing required fields."""
        # Missing display_name
        user_data = {
            "handle": "incomplete.user"
        }
        
        response = self.client.post("/users", json=user_data)
        
        assert response.status_code == 422  # Validation error
        data = response.json()
        
        assert "detail" in data
    
    def test_create_post_success(self):
        """Test successful post creation."""
        # Create user first
        user = self.test_db.create_user(
            handle="poster.user",
            display_name="Poster User"
        )
        
        post_data = {
            "user_id": user.user_id,
            "content": "This is a test post"
        }
        
        response = self.client.post("/posts", json=post_data)
        
        assert response.status_code == 200
        data = response.json()
        
        assert data["user_id"] == user.user_id
        assert data["content"] == "This is a test post"
        assert "post_id" in data
        assert "created_at" in data
        
        # Verify user's score was updated
        user_response = self.client.get(f"/user/{user.handle}")
        user_data = user_response.json()
        assert user_data["score"] == 1
    
    def test_create_post_nonexistent_user(self):
        """Test post creation with nonexistent user."""
        post_data = {
            "user_id": "nonexistent-user-id",
            "content": "This should fail"
        }
        
        response = self.client.post("/posts", json=post_data)
        
        assert response.status_code == 400
        data = response.json()
        
        assert "detail" in data
        assert "nonexistent-user-id" in data["detail"]
    
    def test_create_post_missing_fields(self):
        """Test post creation with missing required fields."""
        # Missing content
        post_data = {
            "user_id": "some-user-id"
        }
        
        response = self.client.post("/posts", json=post_data)
        
        assert response.status_code == 422  # Validation error
        data = response.json()
        
        assert "detail" in data
    
    def test_get_all_users(self):
        """Test retrieving all users."""
        # Create multiple users
        user1 = self.test_db.create_user("user1.test", "User One")
        user2 = self.test_db.create_user("user2.test", "User Two")
        user3 = self.test_db.create_user("user3.test", "User Three")
        
        # Create posts for different scores
        self.test_db.create_post(user1.user_id, "User 1 post")
        self.test_db.create_post(user2.user_id, "User 2 post 1")
        self.test_db.create_post(user2.user_id, "User 2 post 2")
        
        response = self.client.get("/users")
        
        assert response.status_code == 200
        data = response.json()
        
        assert len(data) == 3
        
        # Check that all users are present
        handles = [user["handle"] for user in data]
        assert "user1.test" in handles
        assert "user2.test" in handles
        assert "user3.test" in handles
        
        # Check scores
        user_scores = {user["handle"]: user["score"] for user in data}
        assert user_scores["user1.test"] == 1
        assert user_scores["user2.test"] == 2
        assert user_scores["user3.test"] == 0
    
    def test_recalculate_all_scores(self):
        """Test recalculating all user scores."""
        # Create users
        user1 = self.test_db.create_user("recalc1.test", "Recalc User 1")
        user2 = self.test_db.create_user("recalc2.test", "Recalc User 2")
        
        # Create posts
        self.test_db.create_post(user1.user_id, "User 1 post 1")
        self.test_db.create_post(user1.user_id, "User 1 post 2")
        self.test_db.create_post(user2.user_id, "User 2 post 1")
        
        response = self.client.post("/admin/recalculate-all-scores")
        
        assert response.status_code == 200
        data = response.json()
        
        assert "message" in data
        assert "updated_count" in data
        assert "updated_scores" in data
        assert data["updated_count"] == 2
        assert len(data["updated_scores"]) == 2
        
        # Check that scores were updated
        assert data["updated_scores"][user1.user_id] == 2
        assert data["updated_scores"][user2.user_id] == 1


class TestAPIEdgeCases:
    """Test edge cases and error conditions for API endpoints."""
    
    def setup_method(self):
        """Set up test environment for each test method."""
        # Create temporary database
        self.temp_db = tempfile.NamedTemporaryFile(delete=False)
        self.temp_db.close()
        
        # Mock the database dependency
        self.test_db = Database(self.temp_db.name)
        
        # Override the database dependency in the FastAPI app
        def get_test_database():
            return self.test_db
        
        app.dependency_overrides[app.dependency_overrides.get("get_database", lambda: None)] = get_test_database
        
        # Create test client
        self.client = TestClient(app)
    
    def teardown_method(self):
        """Clean up test environment after each test method."""
        # Remove temporary database file
        os.unlink(self.temp_db.name)
        
        # Clear dependency overrides
        app.dependency_overrides.clear()
    
    def test_user_handle_with_special_characters(self):
        """Test API with user handles containing special characters."""
        # Create user with special characters
        user = self.test_db.create_user(
            handle="user-with_special.chars",
            display_name="Special User"
        )
        
        # Test API endpoint
        response = self.client.get("/user/user-with_special.chars")
        
        assert response.status_code == 200
        data = response.json()
        
        assert data["handle"] == "user-with_special.chars"
        assert data["display_name"] == "Special User"
    
    def test_large_post_content(self):
        """Test creating post with large content."""
        # Create user
        user = self.test_db.create_user(
            handle="large.user",
            display_name="Large User"
        )
        
        # Create post with large content
        large_content = "A" * 5000  # 5KB content
        post_data = {
            "user_id": user.user_id,
            "content": large_content
        }
        
        response = self.client.post("/posts", json=post_data)
        
        assert response.status_code == 200
        data = response.json()
        
        assert len(data["content"]) == 5000
        assert data["content"] == large_content
    
    def test_unicode_content(self):
        """Test API with unicode content."""
        # Create user with unicode
        user_data = {
            "handle": "unicode.user",
            "display_name": "Unicode User 🌟",
            "bio": "I love emojis! 🎉🚀💻"
        }
        
        response = self.client.post("/users", json=user_data)
        
        assert response.status_code == 200
        data = response.json()
        
        assert "🌟" in data["display_name"]
        assert "🎉🚀💻" in data["bio"]
        
        # Create post with unicode
        post_data = {
            "user_id": data["user_id"],
            "content": "Hello world! 🌍 This is a test with émojis: 中文"
        }
        
        response = self.client.post("/posts", json=post_data)
        
        assert response.status_code == 200
        post_data = response.json()
        
        assert "🌍" in post_data["content"]
        assert "émojis" in post_data["content"]
        assert "中文" in post_data["content"]
    
    def test_empty_content_handling(self):
        """Test handling of empty or whitespace-only content."""
        # Create user
        user = self.test_db.create_user(
            handle="empty.user",
            display_name="Empty User"
        )
        
        # Try to create post with empty content
        post_data = {
            "user_id": user.user_id,
            "content": ""
        }
        
        response = self.client.post("/posts", json=post_data)
        
        # Should still work - empty content is technically valid
        assert response.status_code == 200
        data = response.json()
        
        assert data["content"] == ""
    
    def test_invalid_json_request(self):
        """Test API with invalid JSON in request body."""
        response = self.client.post(
            "/users",
            data="invalid json",
            headers={"Content-Type": "application/json"}
        )
        
        assert response.status_code == 422  # Validation error
    
    def test_missing_content_type(self):
        """Test API request without proper content type."""
        user_data = {
            "handle": "test.user",
            "display_name": "Test User"
        }
        
        response = self.client.post("/users", data=str(user_data))
        
        # Should still work with default content type handling
        assert response.status_code == 422  # Validation error for malformed data