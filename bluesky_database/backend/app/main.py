"""Main FastAPI application for the Bluesky Post Explorer Backend.

This module provides the core FastAPI application with authentication endpoints
and data loading integration for preprocessed posts.
"""

from datetime import timedelta
from typing import Annotated, Optional, List, Dict, Any
import sys
import os

# Add the project root to the Python path for imports
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../.."))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from fastapi import Depends, FastAPI, HTTPException, status, Query
from fastapi.responses import JSONResponse, StreamingResponse
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from pydantic import BaseModel
import pandas as pd
import io

from .auth import (
    ACCESS_TOKEN_EXPIRE_MINUTES,
    Token,
    User,
    authenticate_user,
    create_access_token,
    get_current_user,
    verify_token,
)

# Import the load_preprocessed_posts function
try:
    from services.backfill.posts.load_data import load_preprocessed_posts
except ImportError:
    # Fallback for development/testing
    def load_preprocessed_posts(start_date: str, end_date: str, **kwargs):
        """Fallback function for development when services module is not available."""
        import pandas as pd
        # Return mock data for development
        return pd.DataFrame({
            'uri': ['at://example.com/app.bsky.feed.post/123'],
            'text': ['Sample post content'],
            'preprocessing_timestamp': ['2024-01-01T00:00:00Z']
        })

app = FastAPI(
    title="Bluesky Post Explorer Backend",
    description="A FastAPI backend for the Bluesky Post Explorer frontend with authentication, post search, and CSV export.",
    version="1.0.0",
)

# OAuth2 scheme for token authentication
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="auth/login")


# Request/Response Models
class PostSearchRequest(BaseModel):
    """Request model for post search."""
    start_date: str
    end_date: str
    query: Optional[str] = None
    limit: Optional[int] = 100


class PostSearchResponse(BaseModel):
    """Response model for post search."""
    posts: List[Dict[str, Any]]
    total_count: int
    search_params: Dict[str, Any]


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
        "auth": "Authentication endpoints available at /auth/*",
        "endpoints": {
            "search": "/posts/search - Search preprocessed posts",
            "export": "/posts/export - Export posts as CSV"
        }
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


@app.post("/posts/search", response_model=PostSearchResponse)
async def search_posts(
    request: PostSearchRequest,
    current_user: Annotated[User, Depends(get_current_user_dependency)]
) -> PostSearchResponse:
    """Search preprocessed posts within a date range.
    
    Args:
        request: Search parameters including date range and optional query
        current_user: Current authenticated user
        
    Returns:
        PostSearchResponse: Search results with posts and metadata
        
    Raises:
        HTTPException: If data loading fails or invalid parameters
    """
    try:
        # Load preprocessed posts using the integrated function
        posts_df = load_preprocessed_posts(
            start_date=request.start_date,
            end_date=request.end_date,
            output_format="df",
            sorted_by_partition_date=True,
            ascending=False
        )
        
        # Apply text search filter if query provided
        if request.query:
            posts_df = posts_df[
                posts_df['text'].str.contains(request.query, case=False, na=False)
            ]
        
        # Apply limit
        if request.limit:
            posts_df = posts_df.head(request.limit)
        
        # Convert to list of dictionaries for JSON response
        posts_list = posts_df.to_dict(orient="records")
        
        # Handle datetime serialization
        for post in posts_list:
            for key, value in post.items():
                if pd.isna(value):
                    post[key] = None
                elif hasattr(value, 'isoformat'):
                    post[key] = value.isoformat()
        
        return PostSearchResponse(
            posts=posts_list,
            total_count=len(posts_list),
            search_params={
                "start_date": request.start_date,
                "end_date": request.end_date,
                "query": request.query,
                "limit": request.limit
            }
        )
        
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error loading posts: {str(e)}"
        )


@app.get("/posts/export")
async def export_posts(
    start_date: str = Query(..., description="Start date in YYYY-MM-DD format"),
    end_date: str = Query(..., description="End date in YYYY-MM-DD format"),
    query: Optional[str] = Query(None, description="Optional text search query"),
    limit: Optional[int] = Query(None, description="Optional limit on number of posts"),
    current_user: Annotated[User, Depends(get_current_user_dependency)] = Depends()
) -> StreamingResponse:
    """Export preprocessed posts as CSV file.
    
    Args:
        start_date: Start date in YYYY-MM-DD format
        end_date: End date in YYYY-MM-DD format
        query: Optional text search query
        limit: Optional limit on number of posts
        current_user: Current authenticated user
        
    Returns:
        StreamingResponse: CSV file download
        
    Raises:
        HTTPException: If data loading fails or invalid parameters
    """
    try:
        # Load preprocessed posts using the integrated function
        posts_df = load_preprocessed_posts(
            start_date=start_date,
            end_date=end_date,
            output_format="df",
            sorted_by_partition_date=True,
            ascending=False
        )
        
        # Apply text search filter if query provided
        if query:
            posts_df = posts_df[
                posts_df['text'].str.contains(query, case=False, na=False)
            ]
        
        # Apply limit
        if limit:
            posts_df = posts_df.head(limit)
        
        # Convert DataFrame to CSV string
        csv_buffer = io.StringIO()
        posts_df.to_csv(csv_buffer, index=False)
        csv_content = csv_buffer.getvalue()
        
        # Create filename with parameters
        filename_parts = [
            f"posts_{start_date}_to_{end_date}"
        ]
        if query:
            # Sanitize query for filename
            safe_query = "".join(c if c.isalnum() or c in (' ', '-', '_') else '' for c in query)
            filename_parts.append(f"query_{safe_query[:20]}")
        if limit:
            filename_parts.append(f"limit_{limit}")
        
        filename = "_".join(filename_parts) + ".csv"
        
        # Return CSV as streaming response
        return StreamingResponse(
            io.StringIO(csv_content),
            media_type="text/csv",
            headers={"Content-Disposition": f"attachment; filename={filename}"}
        )
        
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error exporting posts: {str(e)}"
        )
