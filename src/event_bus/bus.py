"""
Event Bus using Redis Streams
Provides publish/subscribe functionality for event-driven architecture
"""

import asyncio
import json
import logging
from typing import Callable, Optional, Type, Dict, Any
from datetime import datetime

import redis.asyncio as redis
from redis.exceptions import ResponseError

# Handle imports
try:
    from ..events.base import BaseEvent
except ImportError:
    import sys
    from pathlib import Path
    project_root = Path(__file__).parent.parent.parent
    sys.path.insert(0, str(project_root))
    from src.events.base import BaseEvent

logger = logging.getLogger(__name__)


class EventBus:
    """
    Event Bus using Redis Streams for pub/sub
    
    Features:
    - Publish events to named streams
    - Subscribe with consumer groups
    - Automatic acknowledgment
    - Error handling and retries
    - Stream size management
    """
    
    def __init__(
        self,
        redis_url: str,
        max_stream_length: int = 10000,
        consumer_block_ms: int = 1000,
        batch_size: int = 10
    ):
        """
        Initialize Event Bus
        
        Args:
            redis_url: Redis connection URL
            max_stream_length: Maximum events to keep in stream (MAXLEN)
            consumer_block_ms: Consumer block timeout in milliseconds
            batch_size: Number of events to read per batch
        """
        self.redis_url = redis_url
        self.max_stream_length = max_stream_length
        self.consumer_block_ms = consumer_block_ms
        self.batch_size = batch_size
        
        self.client: Optional[redis.Redis] = None
        self._running = False
        self._tasks: Dict[str, asyncio.Task] = {}
    
    async def connect(self):
        """Connect to Redis"""
        if self.client is None:
            self.client = await redis.from_url(
                self.redis_url,
                encoding="utf-8",
                decode_responses=True
            )
            logger.info(f"✅ Connected to Redis: {self.redis_url}")
    
    async def disconnect(self):
        """Disconnect from Redis"""
        if self.client:
            await self.client.aclose()
            self.client = None
            logger.info("✅ Disconnected from Redis")
    
    async def publish(
        self,
        event: BaseEvent,
        stream_name: str
    ) -> str:
        """
        Publish event to Redis Stream
        
        Args:
            event: Event to publish
            stream_name: Name of the stream (e.g., "ticks", "candles")
            
        Returns:
            Event ID in Redis stream
            
        Example:
            event_id = await bus.publish(tick_event, "ticks")
        """
        if not self.client:
            await self.connect()
        
        try:
            # Serialize event to JSON
            event_data = event.to_json()
            
            # Add to stream with MAXLEN to limit size
            event_id = await self.client.xadd(
                name=stream_name,
                fields={"data": event_data},
                maxlen=self.max_stream_length,
                approximate=True  # Approximate trimming for better performance
            )
            
            logger.debug(
                f"📤 Published {event.event_type} to {stream_name}: {event_id}"
            )
            
            return event_id
            
        except Exception as e:
            logger.error(f"❌ Failed to publish event: {e}")
            raise
    
    async def _ensure_consumer_group(
        self,
        stream_name: str,
        consumer_group: str
    ):
        """
        Ensure consumer group exists
        Creates if doesn't exist (idempotent)
        
        Args:
            stream_name: Stream name
            consumer_group: Consumer group name
        """
        try:
            # Try to create consumer group
            await self.client.xgroup_create(
                name=stream_name,
                groupname=consumer_group,
                id="0",  # Start from beginning
                mkstream=True  # Create stream if doesn't exist
            )
            logger.info(f"✅ Created consumer group '{consumer_group}' on '{stream_name}'")
            
        except ResponseError as e:
            # Group already exists (this is fine)
            if "BUSYGROUP" in str(e):
                logger.debug(f"Consumer group '{consumer_group}' already exists")
            else:
                raise
    
    async def subscribe(
        self,
        stream_name: str,
        consumer_group: str,
        consumer_name: str,
        handler: Callable[[BaseEvent], Any],
        event_type: Type[BaseEvent] = BaseEvent
    ):
        """
        Subscribe to stream and process events
        
        Args:
            stream_name: Stream to subscribe to
            consumer_group: Consumer group name
            consumer_name: This consumer's unique name
            handler: Async function to handle events
            event_type: Event class for deserialization
            
        Example:
            async def handle_tick(event: TickReceivedEvent):
                print(f"Got tick: {event.ltp}")
            
            await bus.subscribe(
                stream_name="ticks",
                consumer_group="candle_builders",
                consumer_name="builder_1",
                handler=handle_tick,
                event_type=TickReceivedEvent
            )
        """
        if not self.client:
            await self.connect()
        
        # Ensure consumer group exists
        await self._ensure_consumer_group(stream_name, consumer_group)
        
        logger.info(
            f"👂 Subscribing to '{stream_name}' as '{consumer_group}:{consumer_name}'"
        )
        
        self._running = True
        
        try:
            while self._running:
                try:
                    # Read from stream with consumer group
                    messages = await self.client.xreadgroup(
                        groupname=consumer_group,
                        consumername=consumer_name,
                        streams={stream_name: ">"},  # ">" means only new messages
                        count=self.batch_size,
                        block=self.consumer_block_ms
                    )
                    
                    if not messages:
                        # No new messages (timeout)
                        continue
                    
                    # Process messages
                    for stream, events in messages:
                        for event_id, event_data in events:
                            try:
                                # Deserialize event
                                event_json = event_data.get("data", "{}")
                                event = event_type.from_json(event_json)
                                
                                # Call handler
                                if asyncio.iscoroutinefunction(handler):
                                    await handler(event)
                                else:
                                    handler(event)
                                
                                # Acknowledge successful processing
                                await self.client.xack(
                                    stream_name,
                                    consumer_group,
                                    event_id
                                )
                                
                                logger.debug(
                                    f"✅ Processed and ACK'd: {event.event_type} ({event_id})"
                                )
                                
                            except Exception as e:
                                logger.error(
                                    f"❌ Error processing event {event_id}: {e}",
                                    exc_info=True
                                )
                                # Don't ACK on error - will be retried
                
                except asyncio.CancelledError:
                    logger.info(f"🛑 Subscription cancelled: {stream_name}")
                    break
                    
                except Exception as e:
                    logger.error(f"❌ Error in subscription loop: {e}", exc_info=True)
                    # Wait before retrying
                    await asyncio.sleep(1)
        
        finally:
            self._running = False
            logger.info(f"🛑 Stopped subscribing to '{stream_name}'")
    
    async def get_stream_info(self, stream_name: str) -> Dict[str, Any]:
        """
        Get information about a stream
        
        Args:
            stream_name: Stream name
            
        Returns:
            Stream info dict
        """
        if not self.client:
            await self.connect()
        
        try:
            info = await self.client.xinfo_stream(stream_name)
            return {
                "length": info.get("length", 0),
                "first_entry": info.get("first-entry"),
                "last_entry": info.get("last-entry"),
                "groups": info.get("groups", 0)
            }
        except ResponseError as e:
            if "no such key" in str(e).lower():
                return {"length": 0, "exists": False}
            raise
    
    async def get_pending_count(
        self,
        stream_name: str,
        consumer_group: str
    ) -> int:
        """
        Get count of pending (unacknowledged) messages
        
        Args:
            stream_name: Stream name
            consumer_group: Consumer group name
            
        Returns:
            Count of pending messages
        """
        if not self.client:
            await self.connect()
        
        try:
            pending = await self.client.xpending(stream_name, consumer_group)
            return pending.get("pending", 0)
        except ResponseError:
            return 0
    
    def stop(self):
        """Stop all subscriptions"""
        self._running = False
        logger.info("🛑 Stopping event bus")


# ========================
# Testing
# ========================
if __name__ == "__main__":
    """
    Test event bus
    Run: uv run python src/event_bus/bus.py
    """
    import sys
    from pathlib import Path
    
    # Add project to path
    project_root = Path(__file__).parent.parent.parent
    sys.path.insert(0, str(project_root))
    
    from src.events.tick_events import TickReceivedEvent
    from src.config.settings import settings
    from decimal import Decimal
    
    async def test_event_bus():
        print("=" * 70)
        print("Event Bus Test")
        print("=" * 70)
        print()
        
        # Create event bus
        bus = EventBus(
            redis_url=settings.get_redis_url,
            max_stream_length=1000,
            consumer_block_ms=1000,
            batch_size=10
        )
        
        # Connect
        await bus.connect()
        print("✅ Connected to Redis")
        print()
        
        # Test 1: Publish events
        print("1. Publishing Test Events:")
        print("-" * 70)
        
        event_ids = []
        for i in range(5):
            tick = TickReceivedEvent(
                instrument_key="TEST_INSTRUMENT",
                raw_timestamp=f"174798484{i}612",
                candle_time=datetime.now(),
                ltp=Decimal(f"18{i}.50"),
                ltq=100 + i,
                volume=1000 * (i + 1),
                oi=10000 * (i + 1)
            )
            
            event_id = await bus.publish(tick, "test_ticks")
            event_ids.append(event_id)
            print(f"   Published event {i+1}: {event_id}")
        
        print()
        
        # Test 2: Stream info
        print("2. Stream Information:")
        print("-" * 70)
        info = await bus.get_stream_info("test_ticks")
        print(f"   Stream length: {info.get('length', 0)}")
        print()
        
        # Test 3: Subscribe and consume
        print("3. Subscribing and Consuming:")
        print("-" * 70)
        
        processed = []
        
        async def handle_tick(event: TickReceivedEvent):
            processed.append(event.event_id)
            print(f"   ✅ Received: LTP={event.ltp}, Volume={event.volume}")
            
            # Stop after processing all
            if len(processed) >= 5:
                bus.stop()
        
        # Subscribe (this will block until stopped)
        try:
            await asyncio.wait_for(
                bus.subscribe(
                    stream_name="test_ticks",
                    consumer_group="test_group",
                    consumer_name="test_consumer",
                    handler=handle_tick,
                    event_type=TickReceivedEvent
                ),
                timeout=5.0
            )
        except asyncio.TimeoutError:
            pass
        
        print()
        print(f"   Processed {len(processed)} events")
        print()
        
        # Test 4: Cleanup
        print("4. Cleanup:")
        print("-" * 70)
        
        # Delete test stream
        await bus.client.delete("test_ticks")
        print("   ✅ Deleted test stream")
        
        await bus.disconnect()
        print("   ✅ Disconnected")
        print()
        
        print("=" * 70)
        print("✅ Event bus test completed!")
        print("=" * 70)
    
    # Run test
    asyncio.run(test_event_bus())