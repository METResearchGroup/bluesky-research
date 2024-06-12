"""Pydantic models for the superposters."""
import re

from pydantic import BaseModel, Field, validator


class SuperposterModel(BaseModel):
    user_did: str = Field(..., description="The user DID.")
    user_handle: str = Field(..., description="The user handle.")
    number_of_posts: int = Field(..., description="The number of posts.")
    superposter_date: str = Field(..., description="The date that the user posted too many posts.")  # noqa
    insert_timestamp: str = Field(..., description="When the superposter was inserted into the database.")  # noqa

    # validate that superposter_date is in YYYY-MM-DD format
    @validator("superposter_date")
    def validate_superposter_date(cls, v):
        if not re.match(r"\d{4}-\d{2}-\d{2}", v):
            raise ValueError("Date must be in YYYY-MM-DD format.")
        return v
