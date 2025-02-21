"""Firehose connection handler for the Bluesky firehose ingestion demo."""

import asyncio
import logging
from typing import AsyncGenerator, Dict, Optional

from atproto import CAR, AtUri, models
from atproto.firehose import FirehoseSubscriber as BaseFirehoseSubscriber
from atproto.firehose.models import MessageFrame

from ..config import settings

logger = logging.getLogger(__name__)

class FirehoseSubscriber:
    """A subscriber for the Bluesky firehose that filters for specific record types."""

    def __init__(self) -> None:
        """Initialize the subscriber."""
        self._subscriber = BaseFirehoseSubscriber()
        self._record_types = set(settings.RECORD_TYPES)

    async def _handle_commit(self, commit: MessageFrame) -> Optional[Dict]:
        """
        Handle a commit message from the firehose.
        
        Args:
            commit: The commit message from the firehose.
            
        Returns:
            Dict: The processed record if it matches our criteria, None otherwise.
        """
        try:
            # Parse the CAR file in the commit
            car = CAR.from_bytes(commit.payload)
            
            # Get the operations from the commit
            ops = models.ComAtprotoSyncSubscribeRepos.Commit(**commit.json()).ops
            
            for op in ops:
                if op.action != 'create':  # We only care about create operations
                    continue
                
                # Get the record from the CAR file
                record = car.blocks.get(op.cid)
                if not record:
                    continue
                
                # Parse the record URI
                uri = AtUri.from_str(op.uri)
                
                # Check if this is a record type we're interested in
                if uri.collection not in self._record_types:
                    continue
                
                # Return the processed record
                return {
                    'type': uri.collection,
                    'uri': str(uri),
                    'cid': str(op.cid),
                    'author': uri.hostname,
                    'record': record
                }
                
        except Exception as e:
            logger.error(f"Error processing commit: {e}")
            return None
            
        return None

    async def subscribe(self) -> AsyncGenerator[Dict, None]:
        """
        Subscribe to the firehose and yield matching records.
        
        Yields:
            Dict: Processed records that match our criteria.
        """
        async for commit in self._subscriber.subscribe():
            if processed := await self._handle_commit(commit):
                yield processed 