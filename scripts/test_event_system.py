"""
Comprehensive Event System Test
Tests the complete event flow: Tick → Candle → Signal
"""

import asyncio
from datetime import datetime
from decimal import Decimal

import sys
from pathlib import Path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.event_bus.bus import EventBus
from src.events.tick_events import TickReceivedEvent
from src.events.candle_events import CandleCompletedEvent
from src.events.signal_events import SellerStateDetectedEvent, BuySignalGeneratedEvent
from src.config.settings import settings
from src.utils.timezone import IST


async def test_complete_flow():
    """Test complete event flow"""
    print("=" * 70)
    print("Complete Event System Test")
    print("=" * 70)
    print()
    
    # Create event bus
    bus = EventBus(
        redis_url=settings.get_redis_url,
        max_stream_length=1000
    )
    
    await bus.connect()
    print("✅ Event bus connected")
    print()
    
    # Counters
    ticks_received = []
    candles_received = []
    signals_received = []
    
    # ========================
    # Handlers
    # ========================
    async def handle_tick(event: TickReceivedEvent):
        ticks_received.append(event)
        print(f"   📊 Tick: {event.instrument_key} @ {event.ltp}")
    
    async def handle_candle(event: CandleCompletedEvent):
        candles_received.append(event)
        print(f"   🕯️  Candle: {event.instrument_key} OHLC={event.open}/{event.high}/{event.low}/{event.close}")
    
    async def handle_signal(event: SellerStateDetectedEvent):
        signals_received.append(event)
        print(f"   🚨 Signal: {event.state} (confidence={event.confidence}) → {event.recommendation}")
    
    # ========================
    # Start consumers
    # ========================
    print("1. Starting Consumers:")
    print("-" * 70)
    
    tick_consumer = asyncio.create_task(
        bus.subscribe("ticks", "tick_handlers", "handler_1", handle_tick, TickReceivedEvent)
    )
    
    candle_consumer = asyncio.create_task(
        bus.subscribe("candles", "candle_handlers", "handler_1", handle_candle, CandleCompletedEvent)
    )
    
    signal_consumer = asyncio.create_task(
        bus.subscribe("signals", "signal_handlers", "handler_1", handle_signal, SellerStateDetectedEvent)
    )
    
    # Wait a bit for consumers to initialize
    await asyncio.sleep(0.5)
    print("   ✅ All consumers started")
    print()
    
    # ========================
    # Publish events
    # ========================
    print("2. Publishing Events:")
    print("-" * 70)
    
    # Publish 3 ticks
    for i in range(3):
        tick = TickReceivedEvent(
            instrument_key="NSE_FO|61755",
            raw_timestamp=f"174798484{i}612",
            candle_time=datetime(2024, 11, 16, 9, 15, 0, tzinfo=IST),
            ltp=Decimal(f"18{i}.50"),
            ltq=100,
            volume=10000 * (i + 1),
            oi=8326800,
            bid_prices=[Decimal("182.00")],
            bid_quantities=[1000],
            ask_prices=[Decimal("182.50")],
            ask_quantities=[800]
        )
        await bus.publish(tick, "ticks")
    
    print("   ✅ Published 3 tick events")
    
    # Publish 1 candle
    candle = CandleCompletedEvent(
        instrument_key="NSE_FO|61755",
        candle_timestamp=datetime(2024, 11, 16, 9, 15, 0, tzinfo=IST),
        open=Decimal("182.00"),
        high=Decimal("183.50"),
        low=Decimal("181.50"),
        close=Decimal("182.50"),
        volume=125000,
        oi=8326800,
        vwap=Decimal("182.25"),
        tick_count=85
    )
    await bus.publish(candle, "candles")
    print("   ✅ Published 1 candle event")
    
    # Publish 1 signal
    signal = SellerStateDetectedEvent(
        instrument_key="NSE_FO|61755",
        detection_timestamp=datetime(2024, 11, 16, 9, 16, 0, tzinfo=IST),
        state="SELLER_PANIC",
        confidence=Decimal("0.85"),
        panic_score=Decimal("75.5"),
        signals=["SHORT_COVERING", "GAMMA_SPIKE"],
        short_covering=True,
        gamma_spike_detected=True,
        recommendation="BUY",
        entry_price=Decimal("182.50")
    )
    await bus.publish(signal, "signals")
    print("   ✅ Published 1 signal event")
    print()
    
    # ========================
    # Wait for processing
    # ========================
    print("3. Processing Events:")
    print("-" * 70)
    await asyncio.sleep(2)
    print()
    
    # ========================
    # Stop consumers
    # ========================
    bus.stop()
    
    # Wait for tasks to finish
    await asyncio.sleep(0.5)
    tick_consumer.cancel()
    candle_consumer.cancel()
    signal_consumer.cancel()
    
    # ========================
    # Results
    # ========================
    print("4. Results:")
    print("-" * 70)
    print(f"   Ticks processed:   {len(ticks_received)}/3")
    print(f"   Candles processed: {len(candles_received)}/1")
    print(f"   Signals processed: {len(signals_received)}/1")
    print()
    
    # ========================
    # Stream Stats
    # ========================
    print("5. Stream Statistics:")
    print("-" * 70)
    
    for stream in ["ticks", "candles", "signals"]:
        info = await bus.get_stream_info(stream)
        print(f"   {stream:15} - {info.get('length', 0)} events")
    
    print()
    
    # ========================
    # Cleanup
    # ========================
    print("6. Cleanup:")
    print("-" * 70)
    await bus.client.delete("ticks", "candles", "signals")
    print("   ✅ Deleted test streams")
    
    await bus.disconnect()
    print("   ✅ Disconnected")
    print()
    
    # ========================
    # Summary
    # ========================
    success = (
        len(ticks_received) == 3 and
        len(candles_received) == 1 and
        len(signals_received) == 1
    )
    
    print("=" * 70)
    if success:
        print("🎉 Complete event system test PASSED!")
    else:
        print("⚠️  Some events not processed")
    print("=" * 70)


if __name__ == "__main__":
    asyncio.run(test_complete_flow())