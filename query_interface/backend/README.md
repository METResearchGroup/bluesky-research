# Query Interface Backend

FastAPI backend service for converting natural language queries to SQL.

## Setup

The backend requires FastAPI and uvicorn, which are available in the project's `feed_api` optional dependencies:

```bash
# Install dependencies (if not already installed)
pip install -e ".[feed_api]"
```

## Running the Server

Run the server locally:

```bash
python query_interface/backend/app.py
```

Or use uvicorn directly:

```bash
uvicorn query_interface.backend.app:app --reload
```

The server will start on `http://127.0.0.1:8000` by default.

## API Endpoints

### GET `/`
Root endpoint that returns a welcome message.

### GET `/health`
Health check endpoint.

### POST `/query`
Converts a natural language query to a mock SQL query.

**Request Body:**
```json
{
  "query": "How many posts were created in the last week?"
}
```

**Response:**
```json
{
  "sql_query": "SELECT COUNT(*) FROM posts WHERE created_at >= CURRENT_DATE - INTERVAL '7 days';",
  "original_query": "How many posts were created in the last week?"
}
```

## API Documentation

Once the server is running, you can access:
- Interactive API docs: `http://127.0.0.1:8000/docs`
- Alternative docs: `http://127.0.0.1:8000/redoc`

