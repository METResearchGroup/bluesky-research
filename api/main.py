"""Main API router for the Bluesky Research Platform.

This module provides the main FastAPI application with user profile endpoints
and other core API functionality.
"""

from typing import Dict, Any

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware

from api.user_router.routes import router as user_router
from api.integrations_router.main import route_and_run_integration_request

app = FastAPI(
    title="Bluesky Research Platform API",
    description="API for managing user profiles and integrations in the Bluesky research platform",
    version="1.0.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET", "POST", "PUT", "DELETE"],
    allow_headers=["*"],
)

# Include user router
app.include_router(user_router, prefix="/user", tags=["users"])


@app.get("/")
async def root():
    """Root endpoint providing API information."""
    return {
        "message": "Bluesky Research Platform API",
        "version": "1.0.0",
        "endpoints": {
            "users": "/user/{handle} - Get user profile by handle",
            "docs": "/docs - API documentation"
        }
    }


@app.get("/health")
async def health_check():
    """Health check endpoint."""
    return {"status": "healthy"}


# Integration endpoint (keeping existing functionality)
@app.post("/integrations")
async def run_integration(request: Dict[str, Any]):
    """Run an integration request."""
    try:
        result = route_and_run_integration_request(request)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8080)