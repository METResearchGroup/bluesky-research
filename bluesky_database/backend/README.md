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