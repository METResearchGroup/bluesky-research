# Bluesky Post Explorer Backend

A FastAPI backend for the Bluesky Post Explorer frontend. Provides authentication, post search, and CSV export. Dockerized and ready for extension.

## Features
- Simple username/password authentication (test-username-2025 / test-password-2025)
- /posts/search and /posts/export endpoints
- Loads data via load_preprocessed_posts
- Dockerized deployment (python:3.10-slim, uv for dependencies)
- All secrets/env from root .env

## Setup

1. Install [uv](https://github.com/astral-sh/uv):
   ```sh
   pip install uv
   ```
2. Install dependencies:
   ```sh
   uv pip install -r requirements.txt
   ```
3. Run the app:
   ```sh
   uvicorn app.main:app --reload
   ```

## Docker Usage

1. Build the image:
   ```sh
   docker build -f Dockerfiles/Dockerfile -t bluesky-backend .
   ```
2. Run the container:
   ```sh
   docker run -p 8000:8000 --env-file ../../.env bluesky-backend
   ```

## Environment
- All secrets and environment variables are read from the root `.env` file.
- Session expiry: 1 hour (default)

## Requirements
- Python 3.10+
- See requirements.in and requirements.txt for dependencies
- Docker (for containerized deployment)

## Directory Structure
- `app/` - FastAPI application code
- `Dockerfiles/` - Dockerfile for backend
- `requirements.in` / `requirements.txt` - Python dependencies 

## Detailed Code Explanation

The backend is organized for clarity and maintainability:

- `app/main.py`: The FastAPI application entrypoint. Defines the root endpoint and will contain all API routes. All functions and classes include type annotations and descriptive docstrings.
- `requirements.in` / `requirements.txt`: Python dependencies. `requirements.txt` is pinned using pip-compile for reproducibility.
- `Dockerfiles/Dockerfile`: Dockerfile for containerized deployment, using python:3.10-slim and uv for dependency management.
- All modules are documented with docstrings, and code is structured for extensibility (e.g., future ML-powered filters).

### Walk-through of Key Modules
- **`app/main.py`**: Initializes the FastAPI app, sets up the root endpoint, and will be extended with authentication, search, and export endpoints. All endpoints are documented with OpenAPI and include type annotations.
- **Configuration**: Environment variables are loaded from the root `.env` file for secrets and configuration.
- **Testing**: Test files (to be added) will cover authentication, data loading, filtering, and API endpoints.

## Testing

To run tests:

1. Activate the environment:
   ```sh
   conda activate bluesky-research
   ```
2. Run tests with pytest:
   ```sh
   pytest bluesky_database/backend/app/tests
   ```

### Test Coverage
- Tests will cover:
  - Authentication logic (login, session, logout)
  - Data loading and filtering
  - API endpoints (`/`, `/posts/search`, `/posts/export`)
  - Error handling and validation
- All test files are located in `bluesky_database/backend/app/tests/` and follow the repo's TDD and documentation standards. 