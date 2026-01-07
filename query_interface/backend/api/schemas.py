"""API endpoint contracts for the query interface backend.

These schemas define the HTTP request/response shapes for the FastAPI endpoints.
They are separate from individual tool output contracts (e.g., SQLGenerationResult,
SQLAnswerabilityResult) which live within their respective tool modules.
"""

from typing import Any

from pydantic import BaseModel


class QueryRequest(BaseModel):
    """Request model for natural language query endpoint."""

    query: str


class QueryResponse(BaseModel):
    """Response model for natural language query endpoint.

    This is the API endpoint contract, not a tool output contract.
    It aggregates results from multiple tools into a
    single HTTP response.
    """

    sql_query: str
    original_query: str
    results: list[dict[str, Any]]
    row_count: int
