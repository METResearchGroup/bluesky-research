"""Integration tests for authentication API endpoints.

This module tests the FastAPI authentication endpoints including:
- Login endpoint
- Logout endpoint  
- User info endpoint
- Authentication verification
"""

import pytest
from fastapi.testclient import TestClient

from bluesky_database.backend.app.main import app

client = TestClient(app)


class TestRootEndpoint:
    """Test the root endpoint."""
    
    def test_root_endpoint(self):
        """Test the root endpoint returns welcome message."""
        response = client.get("/")
        assert response.status_code == 200
        data = response.json()
        assert "Welcome to the Bluesky Post Explorer Backend!" in data["message"]
        assert data["status"] == "healthy"
        assert "auth" in data


class TestLoginEndpoint:
    """Test the login endpoint."""
    
    def test_login_valid_credentials(self):
        """Test login with valid credentials."""
        response = client.post(
            "/auth/login",
            data={
                "username": "test-username-2025",
                "password": "test-password-2025"
            }
        )
        assert response.status_code == 200
        data = response.json()
        assert "access_token" in data
        assert data["token_type"] == "bearer"
        assert len(data["access_token"]) > 0
    
    def test_login_invalid_username(self):
        """Test login with invalid username."""
        response = client.post(
            "/auth/login",
            data={
                "username": "invalid-username",
                "password": "test-password-2025"
            }
        )
        assert response.status_code == 401
        data = response.json()
        assert "Incorrect username or password" in data["detail"]
    
    def test_login_invalid_password(self):
        """Test login with invalid password."""
        response = client.post(
            "/auth/login",
            data={
                "username": "test-username-2025",
                "password": "invalid-password"
            }
        )
        assert response.status_code == 401
        data = response.json()
        assert "Incorrect username or password" in data["detail"]
    
    def test_login_missing_credentials(self):
        """Test login with missing credentials."""
        response = client.post("/auth/login", data={})
        assert response.status_code == 422  # Validation error


class TestProtectedEndpoints:
    """Test protected endpoints that require authentication."""
    
    def get_auth_token(self) -> str:
        """Helper method to get authentication token."""
        response = client.post(
            "/auth/login",
            data={
                "username": "test-username-2025",
                "password": "test-password-2025"
            }
        )
        return response.json()["access_token"]
    
    def test_logout_authenticated(self):
        """Test logout with valid authentication."""
        token = self.get_auth_token()
        response = client.post(
            "/auth/logout",
            headers={"Authorization": f"Bearer {token}"}
        )
        assert response.status_code == 200
        data = response.json()
        assert "logged out successfully" in data["message"]
        assert "test-username-2025" in data["message"]
        assert "discard your access token" in data["detail"]
    
    def test_logout_unauthenticated(self):
        """Test logout without authentication."""
        response = client.post("/auth/logout")
        assert response.status_code == 401
    
    def test_logout_invalid_token(self):
        """Test logout with invalid token."""
        response = client.post(
            "/auth/logout",
            headers={"Authorization": "Bearer invalid-token"}
        )
        assert response.status_code == 401
    
    def test_get_current_user_authenticated(self):
        """Test getting current user info with valid authentication."""
        token = self.get_auth_token()
        response = client.get(
            "/auth/me",
            headers={"Authorization": f"Bearer {token}"}
        )
        assert response.status_code == 200
        data = response.json()
        assert data["username"] == "test-username-2025"
        assert data["email"] == "test@example.com"
        assert data["full_name"] == "Test User"
    
    def test_get_current_user_unauthenticated(self):
        """Test getting current user info without authentication."""
        response = client.get("/auth/me")
        assert response.status_code == 401
    
    def test_get_current_user_invalid_token(self):
        """Test getting current user info with invalid token."""
        response = client.get(
            "/auth/me",
            headers={"Authorization": "Bearer invalid-token"}
        )
        assert response.status_code == 401
    
    def test_verify_auth_authenticated(self):
        """Test authentication verification with valid token."""
        token = self.get_auth_token()
        response = client.get(
            "/auth/verify",
            headers={"Authorization": f"Bearer {token}"}
        )
        assert response.status_code == 200
        data = response.json()
        assert data["authenticated"] is True
        assert data["user"] == "test-username-2025"
        assert "verified" in data["message"]
    
    def test_verify_auth_unauthenticated(self):
        """Test authentication verification without token."""
        response = client.get("/auth/verify")
        assert response.status_code == 401
    
    def test_verify_auth_invalid_token(self):
        """Test authentication verification with invalid token."""
        response = client.get(
            "/auth/verify",
            headers={"Authorization": "Bearer invalid-token"}
        )
        assert response.status_code == 401


class TestAuthenticationFlow:
    """Test complete authentication flow scenarios."""
    
    def test_complete_auth_flow(self):
        """Test complete authentication flow: login -> access protected -> logout."""
        # 1. Login
        login_response = client.post(
            "/auth/login",
            data={
                "username": "test-username-2025",
                "password": "test-password-2025"
            }
        )
        assert login_response.status_code == 200
        token = login_response.json()["access_token"]
        
        # 2. Access protected endpoint
        user_response = client.get(
            "/auth/me",
            headers={"Authorization": f"Bearer {token}"}
        )
        assert user_response.status_code == 200
        assert user_response.json()["username"] == "test-username-2025"
        
        # 3. Verify authentication
        verify_response = client.get(
            "/auth/verify",
            headers={"Authorization": f"Bearer {token}"}
        )
        assert verify_response.status_code == 200
        assert verify_response.json()["authenticated"] is True
        
        # 4. Logout
        logout_response = client.post(
            "/auth/logout",
            headers={"Authorization": f"Bearer {token}"}
        )
        assert logout_response.status_code == 200
        assert "logged out successfully" in logout_response.json()["message"]
    
    def test_token_reuse_after_logout(self):
        """Test that token still works after logout (stateless JWT behavior)."""
        # Note: In a stateless JWT system, tokens remain valid until expiry
        # even after "logout". This is expected behavior for this implementation.
        
        # 1. Login and get token
        login_response = client.post(
            "/auth/login",
            data={
                "username": "test-username-2025",
                "password": "test-password-2025"
            }
        )
        token = login_response.json()["access_token"]
        
        # 2. Logout
        logout_response = client.post(
            "/auth/logout",
            headers={"Authorization": f"Bearer {token}"}
        )
        assert logout_response.status_code == 200
        
        # 3. Token should still work (stateless JWT)
        user_response = client.get(
            "/auth/me",
            headers={"Authorization": f"Bearer {token}"}
        )
        assert user_response.status_code == 200
        assert user_response.json()["username"] == "test-username-2025"
    
    def test_multiple_concurrent_sessions(self):
        """Test multiple concurrent authentication sessions."""
        # Create two separate tokens
        token1_response = client.post(
            "/auth/login",
            data={
                "username": "test-username-2025",
                "password": "test-password-2025"
            }
        )
        token1 = token1_response.json()["access_token"]
        
        # Small delay to ensure different timestamp in JWT
        import time
        time.sleep(1)
        
        token2_response = client.post(
            "/auth/login",
            data={
                "username": "test-username-2025",
                "password": "test-password-2025"
            }
        )
        token2 = token2_response.json()["access_token"]
        
        # Both tokens should work independently
        user1_response = client.get(
            "/auth/me",
            headers={"Authorization": f"Bearer {token1}"}
        )
        assert user1_response.status_code == 200
        
        user2_response = client.get(
            "/auth/me",
            headers={"Authorization": f"Bearer {token2}"}
        )
        assert user2_response.status_code == 200
        
        # Both should return the same user data
        assert user1_response.json()["username"] == "test-username-2025"
        assert user2_response.json()["username"] == "test-username-2025"
        
        # Tokens should be different due to different timestamps
        assert token1 != token2