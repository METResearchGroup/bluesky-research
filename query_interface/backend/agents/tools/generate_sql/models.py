from pydantic import BaseModel, Field


class SQLGenerationResult(BaseModel):
    """Result containing the generated SQL query."""

    sql_query: str = Field(
        description="The generated SQL SELECT query without any markdown formatting or explanations. Only the SQL statement itself."
    )
