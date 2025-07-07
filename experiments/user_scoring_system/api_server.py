"""FastAPI server for the experimental user scoring system.

This module provides the REST API endpoints for accessing user profiles,
scores, and managing users and posts.
"""

import uvicorn
from datetime import datetime
from typing import List, Dict, Any

from fastapi import FastAPI, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from database import Database
from models import (
    User, Post, UserResponse, ScoreResponse, CreateUserRequest, 
    CreatePostRequest, ErrorResponse
)

# Initialize FastAPI app
app = FastAPI(
    title="User Scoring System API",
    description="Experimental API for tracking user scores based on posting activity",
    version="1.0.0"
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET", "POST", "PUT", "DELETE"],
    allow_headers=["*"],
)

# Database dependency
def get_database() -> Database:
    """Get database instance for dependency injection.
    
    Returns:
        Database: Database instance
    """
    return Database()

# Error handler
async def handle_error(error: Exception) -> JSONResponse:
    """Handle errors and return standardized error response.
    
    Args:
        error: The exception that occurred
        
    Returns:
        JSONResponse: Standardized error response
    """
    if isinstance(error, ValueError):
        return JSONResponse(
            status_code=400,
            content=ErrorResponse(
                error="Bad Request",
                detail=str(error),
                code="INVALID_INPUT"
            ).dict()
        )
    else:
        return JSONResponse(
            status_code=500,
            content=ErrorResponse(
                error="Internal Server Error",
                detail="An unexpected error occurred",
                code="INTERNAL_ERROR"
            ).dict()
        )

@app.get("/", tags=["root"])
async def root():
    """Root endpoint with API information.
    
    Returns:
        dict: API information
    """
    return {
        "message": "User Scoring System API",
        "version": "1.0.0",
        "description": "Experimental API for tracking user scores based on posting activity",
        "endpoints": {
            "user_profile": "/user/{handle}",
            "user_score": "/user/{handle}/score",
            "recalculate_score": "/user/{handle}/recalculate",
            "create_user": "/users",
            "create_post": "/posts",
            "all_users": "/users",
            "user_posts": "/user/{handle}/posts"
        }
    }

@app.get("/user/{handle}", response_model=UserResponse, tags=["users"])
async def get_user_profile(handle: str, db: Database = Depends(get_database)):
    """Get user profile by handle.
    
    Args:
        handle: User's handle (with or without @ prefix)
        db: Database instance
        
    Returns:
        UserResponse: User profile with score and post count
        
    Raises:
        HTTPException: If user not found
    """
    try:
        user_profile = db.get_user_profile(handle)
        if not user_profile:
            raise HTTPException(
                status_code=404,
                detail=f"User with handle '{handle}' not found"
            )
        return user_profile
    except Exception as e:
        if isinstance(e, HTTPException):
            raise
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/user/{handle}/score", response_model=ScoreResponse, tags=["users"])
async def get_user_score(handle: str, db: Database = Depends(get_database)):
    """Get user's current score.
    
    Args:
        handle: User's handle (with or without @ prefix)
        db: Database instance
        
    Returns:
        ScoreResponse: User's score information
        
    Raises:
        HTTPException: If user not found
    """
    try:
        score_info = db.get_user_score(handle)
        if not score_info:
            raise HTTPException(
                status_code=404,
                detail=f"User with handle '{handle}' not found"
            )
        return score_info
    except Exception as e:
        if isinstance(e, HTTPException):
            raise
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/user/{handle}/recalculate", response_model=ScoreResponse, tags=["users"])
async def recalculate_user_score(handle: str, db: Database = Depends(get_database)):
    """Recalculate and update user's score.
    
    Args:
        handle: User's handle (with or without @ prefix)
        db: Database instance
        
    Returns:
        ScoreResponse: Updated score information
        
    Raises:
        HTTPException: If user not found
    """
    try:
        user = db.get_user_by_handle(handle)
        if not user:
            raise HTTPException(
                status_code=404,
                detail=f"User with handle '{handle}' not found"
            )
        
        # Recalculate score
        updated_score = db.update_user_score(user.user_id)
        
        # Return updated score info
        score_info = db.get_user_score(handle)
        return score_info
    except Exception as e:
        if isinstance(e, HTTPException):
            raise
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/user/{handle}/posts", response_model=List[Post], tags=["users"])
async def get_user_posts(handle: str, db: Database = Depends(get_database)):
    """Get all posts for a user.
    
    Args:
        handle: User's handle (with or without @ prefix)
        db: Database instance
        
    Returns:
        List[Post]: List of user's posts
        
    Raises:
        HTTPException: If user not found
    """
    try:
        user = db.get_user_by_handle(handle)
        if not user:
            raise HTTPException(
                status_code=404,
                detail=f"User with handle '{handle}' not found"
            )
        
        posts = db.get_user_posts(user.user_id)
        return posts
    except Exception as e:
        if isinstance(e, HTTPException):
            raise
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/users", response_model=User, tags=["users"])
async def create_user(user_request: CreateUserRequest, db: Database = Depends(get_database)):
    """Create a new user.
    
    Args:
        user_request: User creation request data
        db: Database instance
        
    Returns:
        User: Created user object
        
    Raises:
        HTTPException: If user creation fails
    """
    try:
        user = db.create_user(
            handle=user_request.handle,
            display_name=user_request.display_name,
            bio=user_request.bio,
            avatar_url=user_request.avatar_url
        )
        return user
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/posts", response_model=Post, tags=["posts"])
async def create_post(post_request: CreatePostRequest, db: Database = Depends(get_database)):
    """Create a new post.
    
    Args:
        post_request: Post creation request data
        db: Database instance
        
    Returns:
        Post: Created post object
        
    Raises:
        HTTPException: If post creation fails
    """
    try:
        post = db.create_post(
            user_id=post_request.user_id,
            content=post_request.content
        )
        return post
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/users", response_model=List[User], tags=["users"])
async def get_all_users(db: Database = Depends(get_database)):
    """Get all users.
    
    Args:
        db: Database instance
        
    Returns:
        List[User]: List of all users
    """
    try:
        users = db.get_all_users()
        return users
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/admin/recalculate-all-scores", response_model=Dict[str, Any], tags=["admin"])
async def recalculate_all_scores(db: Database = Depends(get_database)):
    """Recalculate scores for all users (admin endpoint).
    
    Args:
        db: Database instance
        
    Returns:
        Dict[str, Any]: Summary of updated scores
    """
    try:
        updated_scores = db.recalculate_all_scores()
        return {
            "message": "All user scores recalculated successfully",
            "updated_count": len(updated_scores),
            "updated_scores": updated_scores
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/health", tags=["health"])
async def health_check():
    """Health check endpoint.
    
    Returns:
        dict: Health status
    """
    return {
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "service": "user-scoring-system"
    }

if __name__ == "__main__":
    uvicorn.run(
        "api_server:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info"
    )