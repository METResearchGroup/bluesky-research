"""Tests for database operations in the experimental user scoring system.

This module tests all database functionality including user creation,
post management, score calculations, and data retrieval.
"""

import pytest
import tempfile
import os
from datetime import datetime
from unittest.mock import patch

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from database import Database
from models import User, Post


class TestDatabase:
    """Test cases for database operations."""
    
    def setup_method(self):
        """Set up test database for each test method."""
        # Create a temporary database file
        self.temp_db = tempfile.NamedTemporaryFile(delete=False)
        self.temp_db.close()
        
        # Initialize database with temporary file
        self.db = Database(self.temp_db.name)
    
    def teardown_method(self):
        """Clean up test database after each test method."""
        # Remove temporary database file
        os.unlink(self.temp_db.name)
    
    def test_database_initialization(self):
        """Test database initialization creates tables correctly."""
        # Test that tables exist by trying to query them
        with self.db.get_connection() as conn:
            cursor = conn.cursor()
            
            # Check users table exists
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='users'")
            assert cursor.fetchone() is not None
            
            # Check posts table exists
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='posts'")
            assert cursor.fetchone() is not None
            
            # Check indexes exist
            cursor.execute("SELECT name FROM sqlite_master WHERE type='index' AND name='idx_users_handle'")
            assert cursor.fetchone() is not None
            
            cursor.execute("SELECT name FROM sqlite_master WHERE type='index' AND name='idx_posts_user_id'")
            assert cursor.fetchone() is not None
    
    def test_create_user_success(self):
        """Test successful user creation."""
        user = self.db.create_user(
            handle="test.user",
            display_name="Test User",
            bio="Test bio",
            avatar_url="https://example.com/avatar.jpg"
        )
        
        assert user.handle == "test.user"
        assert user.display_name == "Test User"
        assert user.bio == "Test bio"
        assert user.avatar_url == "https://example.com/avatar.jpg"
        assert user.score == 0
        assert user.user_id is not None
        assert isinstance(user.created_at, datetime)
        assert isinstance(user.updated_at, datetime)
    
    def test_create_user_duplicate_handle(self):
        """Test that creating a user with duplicate handle raises error."""
        # Create first user
        self.db.create_user(
            handle="duplicate.user",
            display_name="First User"
        )
        
        # Try to create second user with same handle
        with pytest.raises(ValueError, match="User with handle 'duplicate.user' already exists"):
            self.db.create_user(
                handle="duplicate.user",
                display_name="Second User"
            )
    
    def test_get_user_by_handle_success(self):
        """Test successful user retrieval by handle."""
        # Create user
        created_user = self.db.create_user(
            handle="findme.user",
            display_name="Find Me User",
            bio="I can be found"
        )
        
        # Retrieve user
        found_user = self.db.get_user_by_handle("findme.user")
        
        assert found_user is not None
        assert found_user.user_id == created_user.user_id
        assert found_user.handle == "findme.user"
        assert found_user.display_name == "Find Me User"
        assert found_user.bio == "I can be found"
        assert found_user.score == 0
    
    def test_get_user_by_handle_with_at_prefix(self):
        """Test user retrieval with @ prefix in handle."""
        # Create user
        self.db.create_user(
            handle="atprefix.user",
            display_name="At Prefix User"
        )
        
        # Retrieve user with @ prefix
        found_user = self.db.get_user_by_handle("@atprefix.user")
        
        assert found_user is not None
        assert found_user.handle == "atprefix.user"
    
    def test_get_user_by_handle_not_found(self):
        """Test user retrieval when user doesn't exist."""
        user = self.db.get_user_by_handle("nonexistent.user")
        assert user is None
    
    def test_get_user_by_id_success(self):
        """Test successful user retrieval by ID."""
        # Create user
        created_user = self.db.create_user(
            handle="byid.user",
            display_name="By ID User"
        )
        
        # Retrieve user by ID
        found_user = self.db.get_user_by_id(created_user.user_id)
        
        assert found_user is not None
        assert found_user.user_id == created_user.user_id
        assert found_user.handle == "byid.user"
        assert found_user.display_name == "By ID User"
    
    def test_get_user_by_id_not_found(self):
        """Test user retrieval by ID when user doesn't exist."""
        user = self.db.get_user_by_id("nonexistent-id")
        assert user is None
    
    def test_create_post_success(self):
        """Test successful post creation."""
        # Create user first
        user = self.db.create_user(
            handle="poster.user",
            display_name="Poster User"
        )
        
        # Create post
        post = self.db.create_post(
            user_id=user.user_id,
            content="This is a test post"
        )
        
        assert post.user_id == user.user_id
        assert post.content == "This is a test post"
        assert post.post_id is not None
        assert isinstance(post.created_at, datetime)
    
    def test_create_post_nonexistent_user(self):
        """Test post creation with nonexistent user raises error."""
        with pytest.raises(ValueError, match="User with ID 'nonexistent-id' not found"):
            self.db.create_post(
                user_id="nonexistent-id",
                content="This should fail"
            )
    
    def test_create_post_updates_user_score(self):
        """Test that creating a post updates the user's score."""
        # Create user
        user = self.db.create_user(
            handle="scorer.user",
            display_name="Scorer User"
        )
        
        # Verify initial score is 0
        assert user.score == 0
        
        # Create post
        self.db.create_post(
            user_id=user.user_id,
            content="First post"
        )
        
        # Check that user's score was updated
        updated_user = self.db.get_user_by_id(user.user_id)
        assert updated_user.score == 1
    
    def test_get_user_posts(self):
        """Test retrieving all posts for a user."""
        # Create user
        user = self.db.create_user(
            handle="multipost.user",
            display_name="Multi Post User"
        )
        
        # Create multiple posts
        post1 = self.db.create_post(user.user_id, "First post")
        post2 = self.db.create_post(user.user_id, "Second post")
        post3 = self.db.create_post(user.user_id, "Third post")
        
        # Retrieve posts
        posts = self.db.get_user_posts(user.user_id)
        
        assert len(posts) == 3
        # Posts should be ordered by created_at DESC
        assert posts[0].content == "Third post"
        assert posts[1].content == "Second post"
        assert posts[2].content == "First post"
    
    def test_get_user_post_count(self):
        """Test getting post count for a user."""
        # Create user
        user = self.db.create_user(
            handle="counter.user",
            display_name="Counter User"
        )
        
        # Initially should have 0 posts
        assert self.db.get_user_post_count(user.user_id) == 0
        
        # Create posts
        self.db.create_post(user.user_id, "Post 1")
        self.db.create_post(user.user_id, "Post 2")
        self.db.create_post(user.user_id, "Post 3")
        
        # Should now have 3 posts
        assert self.db.get_user_post_count(user.user_id) == 3
    
    def test_calculate_user_score(self):
        """Test score calculation based on post count."""
        # Create user
        user = self.db.create_user(
            handle="calculator.user",
            display_name="Calculator User"
        )
        
        # Initially should have score 0
        assert self.db.calculate_user_score(user.user_id) == 0
        
        # Create posts
        self.db.create_post(user.user_id, "Post 1")
        self.db.create_post(user.user_id, "Post 2")
        self.db.create_post(user.user_id, "Post 3")
        self.db.create_post(user.user_id, "Post 4")
        self.db.create_post(user.user_id, "Post 5")
        
        # Score should equal post count
        assert self.db.calculate_user_score(user.user_id) == 5
    
    def test_update_user_score(self):
        """Test updating user score in database."""
        # Create user
        user = self.db.create_user(
            handle="updater.user",
            display_name="Updater User"
        )
        
        # Create posts manually (without triggering auto-update)
        with self.db.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                INSERT INTO posts (post_id, user_id, content, created_at)
                VALUES (?, ?, ?, ?)
            ''', ("post1", user.user_id, "Manual post 1", datetime.now()))
            cursor.execute('''
                INSERT INTO posts (post_id, user_id, content, created_at)
                VALUES (?, ?, ?, ?)
            ''', ("post2", user.user_id, "Manual post 2", datetime.now()))
            conn.commit()
        
        # User score should still be 0 (not updated yet)
        current_user = self.db.get_user_by_id(user.user_id)
        assert current_user.score == 0
        
        # Update score
        updated_score = self.db.update_user_score(user.user_id)
        assert updated_score == 2
        
        # Verify score was updated in database
        updated_user = self.db.get_user_by_id(user.user_id)
        assert updated_user.score == 2
    
    def test_get_user_profile(self):
        """Test getting complete user profile with score."""
        # Create user
        user = self.db.create_user(
            handle="profile.user",
            display_name="Profile User",
            bio="Profile bio"
        )
        
        # Create posts
        self.db.create_post(user.user_id, "Profile post 1")
        self.db.create_post(user.user_id, "Profile post 2")
        
        # Get user profile
        profile = self.db.get_user_profile("profile.user")
        
        assert profile is not None
        assert profile.user_id == user.user_id
        assert profile.handle == "profile.user"
        assert profile.display_name == "Profile User"
        assert profile.bio == "Profile bio"
        assert profile.score == 2
        assert profile.post_count == 2
    
    def test_get_user_profile_not_found(self):
        """Test getting profile for nonexistent user."""
        profile = self.db.get_user_profile("nonexistent.user")
        assert profile is None
    
    def test_get_user_score_response(self):
        """Test getting user score response."""
        # Create user
        user = self.db.create_user(
            handle="score.user",
            display_name="Score User"
        )
        
        # Create posts
        self.db.create_post(user.user_id, "Score post 1")
        self.db.create_post(user.user_id, "Score post 2")
        self.db.create_post(user.user_id, "Score post 3")
        
        # Get score response
        score_response = self.db.get_user_score("score.user")
        
        assert score_response is not None
        assert score_response.user_id == user.user_id
        assert score_response.handle == "score.user"
        assert score_response.score == 3
        assert score_response.post_count == 3
        assert isinstance(score_response.last_calculated, datetime)
    
    def test_get_all_users(self):
        """Test getting all users from database."""
        # Create multiple users
        user1 = self.db.create_user("user1.test", "User One")
        user2 = self.db.create_user("user2.test", "User Two")
        user3 = self.db.create_user("user3.test", "User Three")
        
        # Get all users
        all_users = self.db.get_all_users()
        
        assert len(all_users) == 3
        
        # Users should be ordered by created_at DESC
        handles = [user.handle for user in all_users]
        assert "user3.test" in handles
        assert "user2.test" in handles
        assert "user1.test" in handles
    
    def test_recalculate_all_scores(self):
        """Test recalculating scores for all users."""
        # Create users
        user1 = self.db.create_user("recalc1.user", "Recalc User 1")
        user2 = self.db.create_user("recalc2.user", "Recalc User 2")
        
        # Create posts
        self.db.create_post(user1.user_id, "User 1 post 1")
        self.db.create_post(user1.user_id, "User 1 post 2")
        self.db.create_post(user2.user_id, "User 2 post 1")
        
        # Manually reset scores to 0
        with self.db.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("UPDATE users SET score = 0")
            conn.commit()
        
        # Recalculate all scores
        updated_scores = self.db.recalculate_all_scores()
        
        assert len(updated_scores) == 2
        assert updated_scores[user1.user_id] == 2
        assert updated_scores[user2.user_id] == 1
        
        # Verify scores were updated in database
        updated_user1 = self.db.get_user_by_id(user1.user_id)
        updated_user2 = self.db.get_user_by_id(user2.user_id)
        
        assert updated_user1.score == 2
        assert updated_user2.score == 1


class TestDatabaseEdgeCases:
    """Test edge cases and error conditions."""
    
    def setup_method(self):
        """Set up test database for each test method."""
        self.temp_db = tempfile.NamedTemporaryFile(delete=False)
        self.temp_db.close()
        self.db = Database(self.temp_db.name)
    
    def teardown_method(self):
        """Clean up test database after each test method."""
        os.unlink(self.temp_db.name)
    
    def test_create_user_minimal_data(self):
        """Test creating user with minimal required data."""
        user = self.db.create_user(
            handle="minimal.user",
            display_name="Minimal User"
        )
        
        assert user.handle == "minimal.user"
        assert user.display_name == "Minimal User"
        assert user.bio is None
        assert user.avatar_url is None
        assert user.score == 0
    
    def test_empty_handle_normalization(self):
        """Test that empty handles are handled correctly."""
        # This should work fine - empty string is a valid handle
        user = self.db.create_user(
            handle="",
            display_name="Empty Handle User"
        )
        
        assert user.handle == ""
        
        # Should be able to retrieve by empty handle
        found_user = self.db.get_user_by_handle("")
        assert found_user is not None
        assert found_user.handle == ""
    
    def test_unicode_content(self):
        """Test handling of unicode characters in content."""
        # Create user
        user = self.db.create_user(
            handle="unicode.user",
            display_name="Unicode User 🌟",
            bio="I love emojis! 🎉🚀💻"
        )
        
        # Create post with unicode content
        post = self.db.create_post(
            user_id=user.user_id,
            content="Hello world! 🌍 This is a test with émojis and spëcial characters: 中文"
        )
        
        assert "🌍" in post.content
        assert "émojis" in post.content
        assert "中文" in post.content
        
        # Verify retrieval works
        retrieved_user = self.db.get_user_by_handle("unicode.user")
        assert "🌟" in retrieved_user.display_name
        assert "🎉🚀💻" in retrieved_user.bio
    
    def test_large_content(self):
        """Test handling of large content."""
        # Create user
        user = self.db.create_user(
            handle="large.user",
            display_name="Large Content User"
        )
        
        # Create post with large content (10KB)
        large_content = "A" * 10000
        post = self.db.create_post(
            user_id=user.user_id,
            content=large_content
        )
        
        assert len(post.content) == 10000
        
        # Verify retrieval works
        posts = self.db.get_user_posts(user.user_id)
        assert len(posts[0].content) == 10000