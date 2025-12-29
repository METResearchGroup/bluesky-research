"""Post processor implementation."""

from typing import Optional

from lib.log.logger import get_logger

from services.sync.stream.core.context import CacheWriteContext
from services.sync.stream.record_processors.protocol import RecordProcessorProtocol
from services.sync.stream.record_processors.transformation.helper import (
    build_record_filename,
    extract_uri_suffix,
)
from services.sync.stream.record_processors.transformation.post import transform_post
from services.sync.stream.record_processors.types import RoutingDecision
from services.sync.stream.core.types import HandlerKey, Operation, RecordType

logger = get_logger(__name__)


class PostProcessor(RecordProcessorProtocol):
    """Processor for post records.

    Handles transformation and routing of post records. Supports multiple
    routing cases:
    1. Study user post → RecordType.POST handler
    2. Reply to study user post → RecordType.REPLY_TO_USER_POST handler
    3. In-network user post → HandlerKey.IN_NETWORK_POST handler
    """

    def transform(self, record: dict, operation: Operation) -> dict:
        """Transform raw firehose post to consolidated post model.

        Args:
            record: Raw firehose post dictionary
            operation: Operation type (CREATE or DELETE)

        Returns:
            Transformed post as dictionary
        """
        return transform_post(record, operation)

    def _handle_study_user_post(
        self,
        author_did: str,
        post_uri: str,
        post_uri_suffix: str,
        filename: str,
        study_user_manager,
    ) -> RoutingDecision:
        """Handle routing decision for study user posts."""
        logger.info(f"Study user {author_did} created a new post: {post_uri_suffix}")
        study_user_manager.insert_study_user_post(
            post_uri=post_uri, user_did=author_did
        )
        return RoutingDecision(
            handler_key=RecordType.POST,
            author_did=author_did,
            filename=filename,
            metadata={"post_uri": post_uri, "post_uri_suffix": post_uri_suffix},
        )

    def _get_reply_author_did(
        self,
        reply_parent: Optional[str],
        reply_root: Optional[str],
        study_user_manager,
    ) -> Optional[str]:
        """Get author DID if reply is to a study user post."""
        if reply_parent:
            author_did = study_user_manager.is_study_user_post(post_uri=reply_parent)
            if author_did:
                return author_did
        if reply_root:
            return study_user_manager.is_study_user_post(post_uri=reply_root)
        return None

    def _handle_reply_to_study_user_post(
        self,
        post_uri: str,
        post_uri_suffix: str,
        filename: str,
        reply_parent: Optional[str],
        reply_root: Optional[str],
        study_user_manager,
    ) -> RoutingDecision:
        """Handle routing decision for replies to study user posts."""
        reply_author_did = self._get_reply_author_did(
            reply_parent, reply_root, study_user_manager
        )

        logger.info(f"Post {post_uri_suffix} is a reply to a post by a study user.")
        return RoutingDecision(
            handler_key=RecordType.REPLY_TO_USER_POST,
            author_did=reply_author_did,
            filename=filename,
            metadata={
                "post_uri": post_uri,
                "post_uri_suffix": post_uri_suffix,
                "reply_parent": reply_parent,
                "reply_root": reply_root,
            },
        )

    def _handle_in_network_post(
        self,
        author_did: str,
        post_uri: str,
        post_uri_suffix: str,
        filename: str,
    ) -> RoutingDecision:
        """Handle routing decision for in-network user posts."""
        logger.info(
            f"In-network user {author_did} created a new post: {post_uri_suffix}"
        )
        return RoutingDecision(
            handler_key=HandlerKey.IN_NETWORK_POST,
            author_did=author_did,
            filename=filename,
            metadata={"post_uri": post_uri, "post_uri_suffix": post_uri_suffix},
        )

    def get_routing_decisions(
        self,
        transformed: dict,
        operation: Operation,
        context: CacheWriteContext,
    ) -> list[RoutingDecision]:
        """Determine routing decisions for a post record.

        Analyzes the post and context to determine which handlers should
        process this post. A single post can route to multiple handlers.

        Args:
            transformed: Transformed post dictionary
            operation: Operation type (CREATE or DELETE)
            context: Cache write context with dependencies

        Returns:
            List of RoutingDecision objects (empty if no routes match)
        """
        decisions: list[RoutingDecision] = []

        if operation == Operation.DELETE:
            # DELETE operations are not currently handled
            return decisions

        study_user_manager = context.study_user_manager
        author_did = transformed["author_did"]
        post_uri = transformed["uri"]
        post_uri_suffix = extract_uri_suffix(post_uri)
        filename = build_record_filename(
            RecordType.POST, Operation.CREATE, author_did, post_uri_suffix
        )

        # Case 1: Check if the post was written by the study user
        if study_user_manager.is_study_user(user_did=author_did):
            decisions.append(
                self._handle_study_user_post(
                    author_did, post_uri, post_uri_suffix, filename, study_user_manager
                )
            )

        # Case 2: Check if the post is a repost of a post written by the study user
        # TODO: Implement repost handling when needed

        # Case 3: Post is a reply to a post written by the study user
        reply_parent = transformed.get("reply_parent")
        reply_root = transformed.get("reply_root")
        if reply_parent or reply_root:
            reply_author_did = self._get_reply_author_did(
                reply_parent, reply_root, study_user_manager
            )
            if reply_author_did is not None:
                decisions.append(
                    self._handle_reply_to_study_user_post(
                        post_uri,
                        post_uri_suffix,
                        filename,
                        reply_parent,
                        reply_root,
                        study_user_manager,
                    )
                )

        # Case 4: Post is written by an in-network user
        if study_user_manager.is_in_network_user(user_did=author_did):
            decisions.append(
                self._handle_in_network_post(
                    author_did, post_uri, post_uri_suffix, filename
                )
            )

        return decisions
