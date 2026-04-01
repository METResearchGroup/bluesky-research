"""FastAPI server for query interface backend.

Takes natural language queries and returns SQL query results.
"""

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware

from query_interface.backend.agents.tools.can_answer_with_sql.exceptions import (
    SQLAnswerabilityError,
)
from query_interface.backend.agents.tools.can_answer_with_sql.tool import (
    can_answer_with_sql,
)
from query_interface.backend.agents.tools.enforce_user_query_constraints.exceptions import (
    UserQueryConstraintError,
)
from query_interface.backend.agents.tools.enforce_user_query_constraints.tool import (
    enforce_user_query_constraints,
)
from query_interface.backend.agents.tools.generate_sql.exceptions import (
    SQLGenerationError,
)
from query_interface.backend.agents.tools.generate_sql.tool import generate_sql
from query_interface.backend.agents.tools.prepare_sql_for_execution.exceptions import (
    SQLPreparationForExecutionError,
)
from query_interface.backend.agents.tools.prepare_sql_for_execution.tool import (
    prepare_sql_for_execution,
)
from query_interface.backend.agents.tools.run_sql_query.exceptions import (
    SQLQueryExecutionError,
)
from query_interface.backend.agents.tools.run_sql_query.tool import run_sql_query
from query_interface.backend.api.schemas import QueryRequest, QueryResponse

app = FastAPI(
    title="Query Interface Backend",
    description="Backend service for converting natural language queries to SQL and executing them",
    version="0.1.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:3000",
        "http://127.0.0.1:3000",
    ],
    allow_credentials=True,
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["*"],
)


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
    """Convert a natural language query to SQL and execute it.

    Args:
        request: The query request containing the natural language query.

    Returns:
        A response containing the generated SQL query, original query, and results.

    Raises:
        HTTPException: If validation fails, query cannot be answered, SQL generation fails, or execution fails.
    """
    try:
        # Step 0: Enforce user query constraints (basic business logic)
        enforce_user_query_constraints(request.query)

        # Step 1: Check if query can be answered with SQL
        answerability_result = can_answer_with_sql(request.query)
        if not answerability_result.can_answer:
            raise HTTPException(
                status_code=400,
                detail=f"Query cannot be answered with SQL: {answerability_result.reason}",
            )

        # Step 2: Generate SQL from natural language query
        generation_result = generate_sql(request.query)
        raw_sql = generation_result.sql_query

        # Step 3: Prepare SQL for execution (clean and enforce LIMIT 10)
        prepared_sql = prepare_sql_for_execution(raw_sql)

        # Step 4: Execute the SQL query
        df = run_sql_query(prepared_sql)

        # Step 5: Convert DataFrame to list of dictionaries for JSON response
        results = df.to_dict(orient="records")

        return QueryResponse(
            sql_query=prepared_sql,
            original_query=request.query,
            results=results,
            row_count=len(df),
        )

    except ValueError as e:
        raise HTTPException(status_code=400, detail=f"Invalid query: {str(e)}")
    except UserQueryConstraintError as e:
        raise HTTPException(
            status_code=400, detail=f"Query constraint violation: {str(e)}"
        )
    except SQLAnswerabilityError as e:
        raise HTTPException(
            status_code=500, detail=f"Answerability check failed: {str(e)}"
        )
    except SQLGenerationError as e:
        raise HTTPException(status_code=500, detail=f"SQL generation failed: {str(e)}")
    except SQLPreparationForExecutionError as e:
        raise HTTPException(status_code=500, detail=f"SQL preparation failed: {str(e)}")
    except SQLQueryExecutionError as e:
        raise HTTPException(status_code=500, detail=f"Query execution failed: {str(e)}")
    except HTTPException:
        # Re-raise HTTP exceptions (like our 400 for unanswerable queries)
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Unexpected error: {str(e)}")


if __name__ == "__main__":
    import os
    import uvicorn

    # Default to loopback to avoid accidentally exposing locally.
    # In containers/Lambda, set HOST=0.0.0.0 if you intend to bind externally.
    host = os.getenv("HOST", "127.0.0.1")
    port = int(os.getenv("PORT", "8000"))
    uvicorn.run(app, host=host, port=port)
