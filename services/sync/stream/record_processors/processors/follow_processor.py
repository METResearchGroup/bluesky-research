"""Follow processor implementation."""

from lib.log.logger import get_logger

from services.sync.stream.context import CacheWriteContext
from services.sync.stream.record_processors.protocol import RecordProcessorProtocol
from services.sync.stream.record_processors.transformation.follow import (
    build_follow_filename,
    transform_follow,
)
from services.sync.stream.record_processors.types import RoutingDecision
from services.sync.stream.types import FollowStatus, Operation, RecordType

logger = get_logger(__name__)


class FollowProcessor(RecordProcessorProtocol):
    """Processor for follow records.

    Handles transformation and routing of follow records. Supports routing for:
    1. Follower is study user → RecordType.FOLLOW handler (FOLLOWER status)
    2. Followee is study user → RecordType.FOLLOW handler (FOLLOWEE status)
    3. Both can be true → returns 2 decisions
    """

    def transform(self, record: dict, operation: Operation) -> dict:
        """Transform raw firehose follow to RawFollow model.

        Args:
            record: Raw firehose follow dictionary
            operation: Operation type (CREATE or DELETE)

        Returns:
            Transformed follow as dictionary
        """
        return transform_follow(record, operation)

    def get_routing_decisions(
        self,
        transformed: dict,
        operation: Operation,
        context: CacheWriteContext,
    ) -> list[RoutingDecision]:
        """Determine routing decisions for a follow record.

        Analyzes the follow and context to determine which handlers should
        process this follow. A single follow can route to multiple handlers
        (if both follower and followee are study users).

        Args:
            transformed: Transformed follow dictionary
            operation: Operation type (CREATE or DELETE)
            context: Cache write context with dependencies

        Returns:
            List of RoutingDecision objects (empty if no routes match)
        """
        decisions: list[RoutingDecision] = []

        if operation == Operation.DELETE:
            # DELETE operations are not currently handled
            # (deleted follows only have URI, no author info)
            return decisions

        study_user_manager = context.study_user_manager
        follower_did = transformed["follower_did"]
        followee_did = transformed["followee_did"]
        filename = build_follow_filename(follower_did, followee_did)

        user_is_follower = study_user_manager.is_study_user(user_did=follower_did)
        user_is_followee = study_user_manager.is_study_user(user_did=followee_did)

        if not user_is_follower and not user_is_followee:
            # This is the expected common case - most follows don't involve study users
            return decisions

        # Someone can follow someone else in the study, in which case both
        # the follower and followee need to be registered.
        if user_is_follower:
            logger.info(f"User {follower_did} followed a new account, {followee_did}.")
            decisions.append(
                RoutingDecision(
                    handler_key=RecordType.FOLLOW,
                    author_did=follower_did,
                    filename=filename,
                    follow_status=FollowStatus.FOLLOWER,
                    metadata={
                        "follower_did": follower_did,
                        "followee_did": followee_did,
                    },
                )
            )

        if user_is_followee:
            logger.info(
                f"User {followee_did} was followed by a new account, {follower_did}."
            )
            decisions.append(
                RoutingDecision(
                    handler_key=RecordType.FOLLOW,
                    author_did=followee_did,
                    filename=filename,
                    follow_status=FollowStatus.FOLLOWEE,
                    metadata={
                        "follower_did": follower_did,
                        "followee_did": followee_did,
                    },
                )
            )

        return decisions
