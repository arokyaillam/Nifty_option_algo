"""
Storage Consumer
Subscribes to Redis streams and persists data to PostgreSQL
"""

import asyncio
from typing import Optional
import logging

import sys
from pathlib import Path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from src.event_bus.bus import EventBus
from src.events.candle_events import CandleCompletedEvent
from src.events.signal_events import SignalGeneratedEvent
from src.database.engine import get_async_session
from src.database.service import DatabaseService
from src.config.settings import settings

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class StorageConsumer:
    """
    Persist events to PostgreSQL
    
    Flow:
    1. Subscribe to "candles" stream → Save to candles table
    2. Subscribe to "signals" stream → Save to signals table
    3. Track seller states → Save to seller_states table
    """
    
    def __init__(self, event_bus: EventBus):
        """
        Initialize storage consumer
        
        Args:
            event_bus: Event bus instance
        """
        self.event_bus = event_bus
        self._running = False
        
        # Statistics
        self.candles_saved = 0
        self.signals_saved = 0
        self.errors = 0
    
    async def _handle_candle(self, candle: CandleCompletedEvent):
        """
        Handle candle event and save to database
        
        Args:
            candle: CandleCompletedEvent
        """
        try:
            async for session in get_async_session():
                service = DatabaseService(session)
                
                saved = await service.save_candle(candle)
                
                if saved:
                    self.candles_saved += 1
                    
                    if self.candles_saved % 10 == 0:
                        logger.info(f"📊 Saved {self.candles_saved} candles to database")
                
                break  # Exit after first session
        
        except Exception as e:
            self.errors += 1
            logger.error(f"❌ Error saving candle: {e}", exc_info=True)
    
    async def _handle_signal(self, signal: SignalGeneratedEvent):
        """
        Handle signal event and save to database
        
        Args:
            signal: SignalGeneratedEvent
        """
        try:
            async for session in get_async_session():
                service = DatabaseService(session)
                
                # Save signal
                saved = await service.save_signal(signal)
                
                if saved:
                    self.signals_saved += 1
                    
                    # Also save seller state
                    await service.save_seller_state(
                        instrument_key=signal.instrument_key,
                        timestamp=signal.signal_timestamp,
                        state=signal.seller_state,
                        panic_score=signal.panic_score,
                        confidence=signal.confidence
                    )
                
                break  # Exit after first session
        
        except Exception as e:
            self.errors += 1
            logger.error(f"❌ Error saving signal: {e}", exc_info=True)
    
    async def start(self):
        """Start storage consumer"""
        await self.event_bus.connect()
        
        logger.info("🚀 Storage consumer started")
        logger.info("   Subscribing to 'candles' and 'signals' streams...")
        
        self._running = True
        
        # Create tasks for both subscriptions
        candle_task = asyncio.create_task(
            self.event_bus.subscribe(
                stream_name="candles",
                consumer_group="storage_consumers",
                consumer_name="storage_candles",
                handler=self._handle_candle,
                event_type=CandleCompletedEvent
            )
        )
        
        signal_task = asyncio.create_task(
            self.event_bus.subscribe(
                stream_name="signals",
                consumer_group="storage_consumers",
                consumer_name="storage_signals",
                handler=self._handle_signal,
                event_type=SignalGeneratedEvent
            )
        )
        
        # Print stats periodically
        async def print_stats():
            while self._running:
                await asyncio.sleep(30)  # Every 30 seconds
                logger.info(
                    f"📈 Storage Stats: "
                    f"Candles={self.candles_saved}, "
                    f"Signals={self.signals_saved}, "
                    f"Errors={self.errors}"
                )
        
        stats_task = asyncio.create_task(print_stats())
        
        # Wait for all tasks
        try:
            await asyncio.gather(candle_task, signal_task, stats_task)
        except asyncio.CancelledError:
            logger.info("🛑 Storage consumer stopped")
    
    def stop(self):
        """Stop storage consumer"""
        self._running = False
        logger.info("🛑 Stopping storage consumer...")


# ========================
# Testing
# ========================
if __name__ == "__main__":
    """
    Test storage consumer
    
    Prerequisites:
    1. Producer running (generating ticks)
    2. Candle builder running (generating candles)
    3. Analysis consumer running (generating signals)
    
    Run: uv run python src/consumers/storage_consumer.py
    """
    
    async def test_storage_consumer():
        print("=" * 70)
        print("Storage Consumer Test")
        print("=" * 70)
        print()
        
        print("This consumer will:")
        print("  • Subscribe to 'candles' stream")
        print("  • Subscribe to 'signals' stream")
        print("  • Save all events to PostgreSQL")
        print()
        print("Prerequisites:")
        print("  1. ✅ Producer running")
        print("  2. ✅ Candle builder running")
        print("  3. ✅ Analysis consumer running")
        print()
        print("Press Ctrl+C to stop")
        print()
        
        bus = EventBus(redis_url=settings.get_redis_url)
        consumer = StorageConsumer(event_bus=bus)
        
        await consumer.start()
    
    asyncio.run(test_storage_consumer())