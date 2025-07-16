"""Authentication utilities for the Bluesky Post Explorer Backend.

This module provides secure authentication functionality including:
- Password hashing and verification
- JWT token generation and validation
- User authentication and session management
"""

from datetime import datetime, timedelta, timezone
from typing import Optional, Union

from fastapi import HTTPException, status
from jose import JWTError, jwt
from passlib.context import CryptContext
from pydantic import BaseModel


# Configuration
SECRET_KEY = "your-secret-key-here-change-in-production"  # TODO: Move to environment variable
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 60  # 1 hour as specified in README

# Password hashing context
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

# Test credentials as specified in README
TEST_USERS = {
    "test-username-2025": {
        "username": "test-username-2025",
        "hashed_password": pwd_context.hash("test-password-2025"),
        "email": "test@example.com",
        "full_name": "Test User",
    }
}


class Token(BaseModel):
    """Token response model."""
    access_token: str
    token_type: str


class TokenData(BaseModel):
    """Token data for validation."""
    username: Optional[str] = None


class User(BaseModel):
    """User model."""
    username: str
    email: Optional[str] = None
    full_name: Optional[str] = None


class UserInDB(User):
    """User model with hashed password."""
    hashed_password: str


def verify_password(plain_password: str, hashed_password: str) -> bool:
    """Verify a plain password against its hash.
    
    Args:
        plain_password: The plain text password to verify
        hashed_password: The hashed password to check against
        
    Returns:
        bool: True if password matches, False otherwise
    """
    return pwd_context.verify(plain_password, hashed_password)


def get_password_hash(password: str) -> str:
    """Generate password hash.
    
    Args:
        password: Plain text password to hash
        
    Returns:
        str: Hashed password
    """
    return pwd_context.hash(password)


def get_user(username: str) -> Optional[UserInDB]:
    """Get user by username.
    
    Args:
        username: Username to lookup
        
    Returns:
        UserInDB: User object if found, None otherwise
    """
    if username in TEST_USERS:
        user_dict = TEST_USERS[username]
        return UserInDB(**user_dict)
    return None


def authenticate_user(username: str, password: str) -> Union[UserInDB, bool]:
    """Authenticate user with username and password.
    
    Args:
        username: Username to authenticate
        password: Plain text password
        
    Returns:
        UserInDB: User object if authentication successful, False otherwise
    """
    user = get_user(username)
    if not user:
        return False
    if not verify_password(password, user.hashed_password):
        return False
    return user


def create_access_token(data: dict, expires_delta: Optional[timedelta] = None) -> str:
    """Create a JWT access token.
    
    Args:
        data: Data to encode in the token
        expires_delta: Optional expiration time delta
        
    Returns:
        str: Encoded JWT token
    """
    to_encode = data.copy()
    if expires_delta:
        expire = datetime.now(timezone.utc) + expires_delta
    else:
        expire = datetime.now(timezone.utc) + timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt


def verify_token(token: str) -> TokenData:
    """Verify and decode JWT token.
    
    Args:
        token: JWT token to verify
        
    Returns:
        TokenData: Token data if valid
        
    Raises:
        HTTPException: If token is invalid
    """
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )
    
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        username: str = payload.get("sub")
        if username is None:
            raise credentials_exception
        token_data = TokenData(username=username)
    except JWTError:
        raise credentials_exception
    
    return token_data


def get_current_user(token: str) -> User:
    """Get current user from token.
    
    Args:
        token: JWT token
        
    Returns:
        User: Current user
        
    Raises:
        HTTPException: If user not found or token invalid
    """
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )
    
    token_data = verify_token(token)
    user = get_user(username=token_data.username)
    if user is None:
        raise credentials_exception
    
    return User(username=user.username, email=user.email, full_name=user.full_name)