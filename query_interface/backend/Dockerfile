FROM python:3.12-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Copy entire project (needed for lib/ and query_interface/ imports)
# This assumes Railway root directory is set to project root
COPY pyproject.toml uv.lock ./
COPY lib /app/lib
COPY query_interface /app/query_interface

# Install uv for dependency management
RUN pip install --no-cache-dir uv

# Install dependencies using uv
# Install from root pyproject.toml with feed_api extra for FastAPI/uvicorn and llm extra for LLM services
RUN uv pip install --system -e ".[feed_api,llm]" --no-cache

# Expose port (Railway will set PORT env var)
EXPOSE 8000

# Run the FastAPI app
# Use 0.0.0.0 to bind to all interfaces (required for Railway)
CMD python -m uvicorn query_interface.backend.app:app --host 0.0.0.0 --port ${PORT:-8000}
