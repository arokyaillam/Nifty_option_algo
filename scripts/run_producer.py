"""
Run Market Data Producer
Start streaming ticks to event bus
"""

import asyncio
import signal
import sys
from pathlib import Path

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.producers.mock_producer import MockTickProducer
from src.producers.upstox_producer import UpstoxProducer
from src.event_bus.bus import EventBus
from src.config.settings import settings
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


async def run_mock_producer():
    """Run mock producer for testing"""
    logger.info("🎭 Starting MOCK producer")
    
    # Create event bus
    bus = EventBus(redis_url=settings.get_redis_url)
    
    # Create producer
    producer = MockTickProducer(
        instrument_key="NSE_FO|61755",  # Example instrument
        base_price=182.00,
        tick_interval=0.1,  # 10 ticks/second
        volatility=0.003,
        event_bus=bus
    )
    
    # Handle shutdown
    def signal_handler(sig, frame):
        logger.info("🛑 Shutdown signal received")
        producer.stop()
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Start
    await producer.start()


async def run_upstox_producer():
    """Run Upstox producer with real market data"""
    logger.info("📡 Starting UPSTOX producer")
    
    # Get access token from environment
    access_token = settings.upstox_access_token
    
    if not access_token:
        logger.error("❌ UPSTOX_ACCESS_TOKEN not set in environment")
        logger.info("   Set it in .env file or environment variable")
        return
    
    # Create event bus
    bus = EventBus(redis_url=settings.get_redis_url)
    
    # Instrument keys (add your instruments here)
    instrument_keys = [
        "NSE_FO|61755",  # Example
        # Add more instruments
    ]
    
    # Create producer
    producer = UpstoxProducer(
        access_token=access_token,
        instrument_keys=instrument_keys,
        event_bus=bus
    )
    
    # Handle shutdown
    def signal_handler(sig, frame):
        logger.info("🛑 Shutdown signal received")
        producer.stop()
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Start
    await producer.start()


if __name__ == "__main__":
    print("=" * 70)
    print("Market Data Producer")
    print("=" * 70)
    print()
    print("Select producer type:")
    print("1. Mock Producer (for testing)")
    print("2. Upstox Producer (real market data)")
    print()
    
    choice = input("Enter choice (1 or 2): ").strip()
    
    if choice == "1":
        asyncio.run(run_mock_producer())
    elif choice == "2":
        asyncio.run(run_upstox_producer())
    else:
        print("❌ Invalid choice")