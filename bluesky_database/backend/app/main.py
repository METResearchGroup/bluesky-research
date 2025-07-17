"""Main FastAPI application for the Bluesky Post Explorer Backend.

This module provides the core FastAPI application with authentication endpoints.
"""

from datetime import timedelta
from typing import Annotated

from fastapi import Depends, FastAPI, HTTPException, status
from fastapi.responses import JSONResponse
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm

from .auth import (
    ACCESS_TOKEN_EXPIRE_MINUTES,
    Token,
    User,
    authenticate_user,
    create_access_token,
    get_current_user,
    verify_token,
)

app = FastAPI(
    title="Bluesky Post Explorer Backend",
    description="A FastAPI backend for the Bluesky Post Explorer frontend with authentication, post search, and CSV export.",
    version="1.0.0",
)

# OAuth2 scheme for token authentication
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="auth/login")


async def get_current_user_dependency(token: Annotated[str, Depends(oauth2_scheme)]) -> User:
    """Dependency to get current authenticated user.
    
    Args:
        token: JWT token from OAuth2 scheme
        
    Returns:
        User: Current authenticated user
        
    Raises:
        HTTPException: If authentication fails
    """
    return get_current_user(token)


@app.get("/", response_class=JSONResponse)
def root() -> dict:
    """Root endpoint for health check and welcome message.
    
    Returns:
        dict: Welcome message and status
    """
    return {
        "message": "Welcome to the Bluesky Post Explorer Backend!",
        "status": "healthy",
        "auth": "Authentication endpoints available at /auth/*"
    }


@app.post("/auth/login", response_model=Token)
async def login(form_data: Annotated[OAuth2PasswordRequestForm, Depends()]) -> Token:
    """Authenticate user and return access token.
    
    Args:
        form_data: Username and password from OAuth2 form
        
    Returns:
        Token: Access token and token type
        
    Raises:
        HTTPException: If authentication fails
    """
    user = authenticate_user(form_data.username, form_data.password)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )
    
    access_token_expires = timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    access_token = create_access_token(
        data={"sub": user.username}, expires_delta=access_token_expires
    )
    
    return Token(access_token=access_token, token_type="bearer")


@app.post("/auth/logout")
async def logout(current_user: Annotated[User, Depends(get_current_user_dependency)]) -> dict:
    """Logout current user.
    
    Note: Since we're using stateless JWT tokens, logout is handled client-side
    by discarding the token. In a production system, you might want to implement
    a token blacklist for enhanced security.
    
    Args:
        current_user: Current authenticated user
        
    Returns:
        dict: Logout confirmation message
    """
    return {
        "message": f"User {current_user.username} logged out successfully",
        "detail": "Please discard your access token"
    }


@app.get("/auth/me", response_model=User)
async def get_current_user_info(current_user: Annotated[User, Depends(get_current_user_dependency)]) -> User:
    """Get current authenticated user information.
    
    Args:
        current_user: Current authenticated user
        
    Returns:
        User: Current user information
    """
    return current_user


@app.get("/auth/verify")
async def verify_auth(current_user: Annotated[User, Depends(get_current_user_dependency)]) -> dict:
    """Verify authentication status.
    
    This endpoint can be used by the frontend to check if the user is still authenticated.
    
    Args:
        current_user: Current authenticated user
        
    Returns:
        dict: Authentication status and user info
    """
    return {
        "authenticated": True,
        "user": current_user.username,
        "message": "Authentication verified"
    }
