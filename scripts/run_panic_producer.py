"""
Run Panic Scenario Producer
Generates realistic panic conditions for BUY signal testing
"""

import asyncio
import signal
import sys
from pathlib import Path

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.producers.panic_mock_producer import PanicMockProducer
from src.event_bus.bus import EventBus
from src.config.settings import settings
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

async def main():
    print("=" * 70)
    print("Panic Scenario Producer")
    print("=" * 70)
    print()
    print("This producer will generate:")
    print("  • Normal market conditions (70%)")
    print("  • Panic scenarios (30%):")
    print("    - Short covering")
    print("    - Gamma squeeze")
    print("    - Liquidity crisis")
    print()
    print("🎯 Watch for BUY signals in analysis consumer!")
    print()
    
    bus = EventBus(redis_url=settings.get_redis_url)
    producer = PanicMockProducer(
        event_bus=bus,
        panic_probability=0.25  # 25% chance per minute
    )
    
    def signal_handler(sig, frame):
        producer.stop()
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    await producer.start()

if __name__ == "__main__":
    asyncio.run(main())