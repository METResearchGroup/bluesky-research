# syntax=docker/dockerfile:1
FROM python:3.10-slim

# Install uv (ultra-fast Python package manager)
RUN pip install --no-cache-dir uv

# Set workdir
WORKDIR /app

# Copy requirements files
COPY requirements.in requirements.txt ./

# Install dependencies with uv
RUN uv pip install --system -r requirements.txt

# Copy app code
COPY app ./app

# Expose port
EXPOSE 8000

# Entrypoint
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"] 