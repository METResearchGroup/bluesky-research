"""FastAPI server for query interface backend.

Takes natural language queries and returns mock SQL queries.
"""

from fastapi import FastAPI
from pydantic import BaseModel

app = FastAPI(
    title="Query Interface Backend",
    description="Backend service for converting natural language queries to SQL",
    version="0.1.0",
)


class QueryRequest(BaseModel):
    """Request model for natural language query."""

    query: str


class QueryResponse(BaseModel):
    """Response model containing the generated SQL query."""

    sql_query: str
    original_query: str


@app.get("/")
async def root():
    """Root endpoint."""
    return {"message": "Query Interface Backend API"}


@app.get("/health")
async def health():
    """Health check endpoint."""
    return {"status": "healthy"}


@app.post("/query", response_model=QueryResponse)
async def convert_query(request: QueryRequest) -> QueryResponse:
    """Convert a natural language query to a mock SQL query.

    Args:
        request: The query request containing the natural language query.

    Returns:
        A response containing the mock SQL query and original query.
    """
    # Simple mock SQL generation based on keywords in the query
    query_lower = request.query.lower()

    # Basic keyword detection for mock SQL generation
    if "count" in query_lower or "how many" in query_lower:
        sql = "SELECT COUNT(*) FROM posts WHERE created_at >= CURRENT_DATE - INTERVAL '7 days';"
    elif "average" in query_lower or "avg" in query_lower or "mean" in query_lower:
        sql = "SELECT AVG(like_count) FROM posts WHERE created_at >= CURRENT_DATE - INTERVAL '30 days';"
    elif "top" in query_lower or "most" in query_lower:
        sql = "SELECT * FROM posts ORDER BY like_count DESC LIMIT 10;"
    elif "user" in query_lower or "author" in query_lower:
        sql = "SELECT DISTINCT author_did FROM posts WHERE created_at >= CURRENT_DATE - INTERVAL '1 day';"
    else:
        # Default mock SQL query
        sql = "SELECT * FROM posts WHERE created_at >= CURRENT_DATE - INTERVAL '7 days' LIMIT 100;"

    return QueryResponse(sql_query=sql, original_query=request.query)


if __name__ == "__main__":
    import os
    import uvicorn

    # Default to loopback to avoid accidentally exposing locally.
    # In containers/Lambda, set HOST=0.0.0.0 if you intend to bind externally.
    host = os.getenv("HOST", "127.0.0.1")
    port = int(os.getenv("PORT", "8000"))
    uvicorn.run(app, host=host, port=port)

