"""Like processor implementation."""

from typing import Optional

from lib.log.logger import get_logger

from services.sync.stream.context import CacheWriteContext
from services.sync.stream.record_processors.protocol import RecordProcessorProtocol
from services.sync.stream.record_processors.transformation.like import (
    build_like_filename,
    extract_like_uri_suffix,
    extract_liked_post_uri,
    transform_like,
)
from services.sync.stream.record_processors.types import RoutingDecision
from services.sync.stream.types import Operation, RecordType

logger = get_logger(__name__)


class LikeProcessor(RecordProcessorProtocol):
    """Processor for like records.

    Handles transformation and routing of like records. Supports two routing cases:
    1. Study user likes a post → RecordType.LIKE handler
    2. Post liked by study user → RecordType.LIKE_ON_USER_POST handler
    """

    def transform(self, record: dict, operation: Operation) -> dict:
        """Transform raw firehose like to RawLike model.

        Args:
            record: Raw firehose like dictionary
            operation: Operation type (CREATE or DELETE)

        Returns:
            Transformed like as dictionary
        """
        return transform_like(record, operation)

    def get_routing_decisions(
        self,
        transformed: dict,
        operation: Operation,
        context: CacheWriteContext,
    ) -> list[RoutingDecision]:
        """Determine routing decisions for a like record.

        Analyzes the like and context to determine which handlers should
        process this like. A single like can route to multiple handlers.

        Args:
            transformed: Transformed like dictionary
            operation: Operation type (CREATE or DELETE)
            context: Cache write context with dependencies

        Returns:
            List of RoutingDecision objects (empty if no routes match)
        """
        decisions: list[RoutingDecision] = []

        if operation == Operation.DELETE:
            # DELETE operations are not currently handled
            # (deleted likes only have URI, no author info)
            return decisions

        study_user_manager = context.study_user_manager
        like_author_did = transformed["author"]
        like_uri = transformed["uri"]
        like_uri_suffix = extract_like_uri_suffix(like_uri)
        filename = build_like_filename(like_author_did, like_uri_suffix)

        # Case 1: The user is the one who likes a post
        is_study_user = study_user_manager.is_study_user(user_did=like_author_did)
        if is_study_user:
            logger.info(f"Exporting like data for user {like_author_did}")
            decisions.append(
                RoutingDecision(
                    handler_key=RecordType.LIKE,
                    author_did=like_author_did,
                    filename=filename,
                    metadata={"like_uri": like_uri, "like_uri_suffix": like_uri_suffix},
                )
            )

        # Case 2: The user is the one who created the post that was liked
        # NOTE: This doesn't backfill with a user's past posts, so we only
        # have posts that were created after the user was added to the study
        # and the firehose was run.
        liked_post_uri = extract_liked_post_uri(transformed)
        liked_post_is_study_user_post: Optional[str] = (
            study_user_manager.is_study_user_post(post_uri=liked_post_uri)
        )

        if liked_post_is_study_user_post:
            logger.info(f"Exporting like data for post {liked_post_uri}")
            decisions.append(
                RoutingDecision(
                    handler_key=RecordType.LIKE_ON_USER_POST,
                    author_did=liked_post_is_study_user_post,
                    filename=filename,
                    metadata={
                        "like_uri": like_uri,
                        "like_uri_suffix": like_uri_suffix,
                        "liked_post_uri": liked_post_uri,
                    },
                )
            )

        return decisions
