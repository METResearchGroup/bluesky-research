"""Repositories for rank_score_feeds service."""

from services.rank_score_feeds.repositories.scores_repo import (
    ScoresRepository,
    ScoresRepositoryProtocol,
)

__all__ = [
    "ScoresRepository",
    "ScoresRepositoryProtocol",
]
