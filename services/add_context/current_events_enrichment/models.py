"""Pydantic models for the current events enrichment service."""
from typing import Optional

from pydantic import BaseModel, Field


class NewsOutletModel(BaseModel):
    """Pydantic model for the news outlet."""
    outlet_id: str = Field(
        description="The ID of the news outlet."
    )
    domain: str = Field(
        description="The domain of the news outlet."
    )
    political_party: str = Field(
        description="The political party of the news outlet."
    )
    synctimestamp: str = Field(
        description="The timestamp of the sync."
    )


class NewsArticleModel(BaseModel):
    """Pydantic model for the news article."""
    url: str = Field(
        description="The URL of the news article."
    )
    title: str = Field(
        description="The title of the news article."
    )
    content: Optional[str] = Field(
        description="The content of the news article."
    )
    description: Optional[str] = Field(
        description="The description of the news article."
    )
    publishedAt: str = Field(
        description="The published date of the news article."
    )
    news_outlet_source_id: str = Field(
        description="The news outlet source ID of the news article."
    )
    synctimestamp: str = Field(
        description="The timestamp of the sync."
    )
