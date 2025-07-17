"""Unit tests for authentication utilities.

This module tests the core authentication functionality including:
- Password hashing and verification
- JWT token generation and validation
- User authentication and retrieval
"""

import pytest
from datetime import datetime, timedelta, timezone
from jose import jwt, JWTError

from bluesky_database.backend.app.auth import (
    SECRET_KEY,
    ALGORITHM,
    ACCESS_TOKEN_EXPIRE_MINUTES,
    verify_password,
    get_password_hash,
    get_user,
    authenticate_user,
    create_access_token,
    verify_token,
    get_current_user,
    TokenData,
    User,
    UserInDB,
)
from fastapi import HTTPException


class TestPasswordUtils:
    """Test password hashing and verification utilities."""
    
    def test_verify_password_correct(self):
        """Test password verification with correct password."""
        password = "test-password-123"
        hashed = get_password_hash(password)
        assert verify_password(password, hashed) is True
    
    def test_verify_password_incorrect(self):
        """Test password verification with incorrect password."""
        password = "test-password-123"
        wrong_password = "wrong-password"
        hashed = get_password_hash(password)
        assert verify_password(wrong_password, hashed) is False
    
    def test_get_password_hash_generates_different_hashes(self):
        """Test that password hashing generates different hashes for same password."""
        password = "test-password-123"
        hash1 = get_password_hash(password)
        hash2 = get_password_hash(password)
        # Different salts should produce different hashes
        assert hash1 != hash2
        # But both should verify correctly
        assert verify_password(password, hash1) is True
        assert verify_password(password, hash2) is True


class TestUserManagement:
    """Test user retrieval and authentication."""
    
    def test_get_user_existing(self):
        """Test retrieving an existing user."""
        user = get_user("test-username-2025")
        assert user is not None
        assert isinstance(user, UserInDB)
        assert user.username == "test-username-2025"
        assert user.email == "test@example.com"
        assert user.full_name == "Test User"
        assert user.hashed_password is not None
    
    def test_get_user_nonexistent(self):
        """Test retrieving a non-existent user."""
        user = get_user("nonexistent-user")
        assert user is None
    
    def test_authenticate_user_valid_credentials(self):
        """Test user authentication with valid credentials."""
        user = authenticate_user("test-username-2025", "test-password-2025")
        assert user is not False
        assert isinstance(user, UserInDB)
        assert user.username == "test-username-2025"
    
    def test_authenticate_user_invalid_username(self):
        """Test user authentication with invalid username."""
        user = authenticate_user("invalid-username", "test-password-2025")
        assert user is False
    
    def test_authenticate_user_invalid_password(self):
        """Test user authentication with invalid password."""
        user = authenticate_user("test-username-2025", "invalid-password")
        assert user is False


class TestTokenManagement:
    """Test JWT token generation and validation."""
    
    def test_create_access_token_default_expiry(self):
        """Test access token creation with default expiry."""
        data = {"sub": "test-username-2025"}
        token = create_access_token(data)
        
        # Decode token to verify contents
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        assert payload["sub"] == "test-username-2025"
        
        # Verify expiry is set correctly
        exp_timestamp = payload["exp"]
        exp_datetime = datetime.fromtimestamp(exp_timestamp, tz=timezone.utc)
        now = datetime.now(timezone.utc)
        expected_exp = now + timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
        
        # Allow for some variance (within 1 minute)
        assert abs((exp_datetime - expected_exp).total_seconds()) < 60
    
    def test_create_access_token_custom_expiry(self):
        """Test access token creation with custom expiry."""
        data = {"sub": "test-username-2025"}
        custom_expiry = timedelta(minutes=30)
        token = create_access_token(data, expires_delta=custom_expiry)
        
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        exp_timestamp = payload["exp"]
        exp_datetime = datetime.fromtimestamp(exp_timestamp, tz=timezone.utc)
        now = datetime.now(timezone.utc)
        expected_exp = now + custom_expiry
        
        # Allow for some variance (within 1 minute)
        assert abs((exp_datetime - expected_exp).total_seconds()) < 60
    
    def test_verify_token_valid(self):
        """Test token verification with valid token."""
        data = {"sub": "test-username-2025"}
        token = create_access_token(data)
        
        token_data = verify_token(token)
        assert isinstance(token_data, TokenData)
        assert token_data.username == "test-username-2025"
    
    def test_verify_token_invalid_signature(self):
        """Test token verification with invalid signature."""
        # Create a token with wrong secret
        data = {"sub": "test-username-2025"}
        wrong_token = jwt.encode(data, "wrong-secret", algorithm=ALGORITHM)
        
        with pytest.raises(HTTPException) as exc_info:
            verify_token(wrong_token)
        
        assert exc_info.value.status_code == 401
        assert "Could not validate credentials" in exc_info.value.detail
    
    def test_verify_token_expired(self):
        """Test token verification with expired token."""
        data = {"sub": "test-username-2025"}
        expired_token = create_access_token(data, expires_delta=timedelta(seconds=-1))
        
        with pytest.raises(HTTPException) as exc_info:
            verify_token(expired_token)
        
        assert exc_info.value.status_code == 401
    
    def test_verify_token_missing_subject(self):
        """Test token verification with missing subject."""
        data = {"not_sub": "test-username-2025"}  # Wrong key
        token = jwt.encode(data, SECRET_KEY, algorithm=ALGORITHM)
        
        with pytest.raises(HTTPException) as exc_info:
            verify_token(token)
        
        assert exc_info.value.status_code == 401
    
    def test_verify_token_malformed(self):
        """Test token verification with malformed token."""
        malformed_token = "not-a-jwt-token"
        
        with pytest.raises(HTTPException) as exc_info:
            verify_token(malformed_token)
        
        assert exc_info.value.status_code == 401


class TestCurrentUser:
    """Test current user retrieval from token."""
    
    def test_get_current_user_valid_token(self):
        """Test getting current user with valid token."""
        data = {"sub": "test-username-2025"}
        token = create_access_token(data)
        
        user = get_current_user(token)
        assert isinstance(user, User)
        assert user.username == "test-username-2025"
        assert user.email == "test@example.com"
        assert user.full_name == "Test User"
    
    def test_get_current_user_invalid_token(self):
        """Test getting current user with invalid token."""
        invalid_token = "invalid-token"
        
        with pytest.raises(HTTPException) as exc_info:
            get_current_user(invalid_token)
        
        assert exc_info.value.status_code == 401
    
    def test_get_current_user_nonexistent_user(self):
        """Test getting current user for non-existent user."""
        data = {"sub": "nonexistent-user"}
        token = create_access_token(data)
        
        with pytest.raises(HTTPException) as exc_info:
            get_current_user(token)
        
        assert exc_info.value.status_code == 401