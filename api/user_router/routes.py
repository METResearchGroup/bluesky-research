"""User API routes for the Bluesky Research Platform.

This module provides endpoints for accessing user profiles and scores.
"""

from typing import Optional, Dict, Any

from fastapi import APIRouter, HTTPException, Depends
from fastapi.responses import JSONResponse

from services.participant_data.helper import get_user_profile_response
from services.participant_data.models import UserProfileResponse
from lib.log.logger import get_logger

logger = get_logger(__name__)

router = APIRouter()


@router.get("/{handle}", response_model=UserProfileResponse)
async def get_user_profile(handle: str) -> UserProfileResponse:
    """Get user profile by Bluesky handle.
    
    Args:
        handle: The user's Bluesky handle (without @)
        
    Returns:
        UserProfileResponse: User profile data including score
        
    Raises:
        HTTPException: 404 if user not found
    """
    logger.info(f"Fetching user profile for handle: {handle}")
    
    # Remove @ prefix if present
    clean_handle = handle.lstrip('@')
    
    try:
        user_profile = get_user_profile_response(clean_handle)
        
        if not user_profile:
            logger.warning(f"User not found with handle: {clean_handle}")
            raise HTTPException(
                status_code=404, 
                detail=f"User with handle '{clean_handle}' not found"
            )
        
        logger.info(f"Successfully retrieved profile for user: {clean_handle}, score: {user_profile.score}")
        return user_profile
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error retrieving user profile for {clean_handle}: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail="Internal server error while retrieving user profile"
        )


@router.get("/{handle}/score")
async def get_user_score(handle: str) -> Dict[str, Any]:
    """Get just the user's score by Bluesky handle.
    
    Args:
        handle: The user's Bluesky handle (without @)
        
    Returns:
        dict: User's score information
        
    Raises:
        HTTPException: 404 if user not found
    """
    logger.info(f"Fetching user score for handle: {handle}")
    
    # Remove @ prefix if present
    clean_handle = handle.lstrip('@')
    
    try:
        user_profile = get_user_profile_response(clean_handle)
        
        if not user_profile:
            logger.warning(f"User not found with handle: {clean_handle}")
            raise HTTPException(
                status_code=404, 
                detail=f"User with handle '{clean_handle}' not found"
            )
        
        return {
            "bluesky_handle": user_profile.bluesky_handle,
            "score": user_profile.score
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error retrieving user score for {clean_handle}: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail="Internal server error while retrieving user score"
        )