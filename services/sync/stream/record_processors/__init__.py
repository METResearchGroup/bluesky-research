"""Record processors for firehose stream data filtering."""

from services.sync.stream.record_processors.factories import create_all_processors
from services.sync.stream.record_processors.processors import (
    FollowProcessor,
    LikeProcessor,
    PostProcessor,
)
from services.sync.stream.record_processors.protocol import RecordProcessorProtocol
from services.sync.stream.record_processors.registry import ProcessorRegistry
from services.sync.stream.record_processors.router import route_decisions
from services.sync.stream.record_processors.types import RoutingDecision

__all__ = [
    "RecordProcessorProtocol",
    "ProcessorRegistry",
    "RoutingDecision",
    "PostProcessor",
    "LikeProcessor",
    "FollowProcessor",
    "create_all_processors",
    "route_decisions",
]
