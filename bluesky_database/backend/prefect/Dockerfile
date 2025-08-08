FROM python:3.12-slim

# Install system dependencies
RUN apt-get update && apt-get install -y \
    curl \
    wget \
    procps \
    && rm -rf /var/lib/apt/lists/*

# Install uv
RUN pip install uv

# Set working directory
WORKDIR /app

# Copy requirements.in
COPY prefect/requirements.in .

# Create virtual environment and install dependencies using uv
RUN uv venv && uv pip install -r requirements.in

# Install Prefect CLI
RUN uv pip install prefect

# Activate virtual environment in shell
ENV PATH="/app/.venv/bin:$PATH"

# Copy Prefect code
COPY prefect/ ./prefect/

# Set environment variables
ENV PYTHONPATH=/app
ENV PREFECT_API_URL=http://localhost:4200/api

# Expose port
EXPOSE 4200

# Default command (can be overridden)
CMD ["prefect", "server", "start"]
