"""
Mock Tick Producer
Generates realistic tick data for testing without Upstox API
"""

import asyncio
import random
from datetime import datetime
from decimal import Decimal
from typing import List

import sys
from pathlib import Path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from src.event_bus.bus import EventBus
from src.events.tick_events import TickReceivedEvent
from src.config.settings import settings
from src.utils.timezone import now_ist, candle_minute, is_trading_time
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class MockTickProducer:
    """
    Generate mock tick data for testing
    
    Features:
    - Realistic price movements
    - 30-level order book
    - Greeks simulation
    - Configurable tick rate
    """
    
    def __init__(
        self,
        instrument_key: str = "NSE_FO|61755",
        base_price: float = 182.00,
        tick_interval: float = 0.1,  # seconds between ticks
        volatility: float = 0.002,  # 0.2% volatility
        event_bus: EventBus = None
    ):
        """
        Initialize mock producer
        
        Args:
            instrument_key: Instrument identifier
            base_price: Starting price
            tick_interval: Seconds between ticks
            volatility: Price volatility (percentage)
            event_bus: Event bus instance
        """
        self.instrument_key = instrument_key
        self.base_price = base_price
        self.current_price = base_price
        self.tick_interval = tick_interval
        self.volatility = volatility
        self.event_bus = event_bus
        
        self.volume = 0
        self.oi = 8326800
        self.tick_count = 0
        
        self._running = False
    
    def _generate_price_movement(self) -> Decimal:
        """
        Generate next price with random walk
        
        Returns:
            New price
        """
        # Random walk with mean reversion
        change_pct = random.gauss(0, self.volatility)
        
        # Mean reversion force
        mean_reversion = (self.base_price - self.current_price) * 0.01
        
        # New price
        change = self.current_price * change_pct + mean_reversion
        new_price = self.current_price + change
        
        # Ensure positive and reasonable
        new_price = max(new_price, self.base_price * 0.7)
        new_price = min(new_price, self.base_price * 1.3)
        
        return Decimal(str(round(new_price, 2)))
    
    def _generate_order_book(
        self,
        mid_price: Decimal
    ) -> tuple:
        """
        Generate 30-level order book around mid price
        
        Args:
            mid_price: Current mid price
            
        Returns:
            (bid_prices, bid_quantities, ask_prices, ask_quantities)
        """
        bid_prices = []
        bid_quantities = []
        ask_prices = []
        ask_quantities = []
        
        # Spread (0.1% to 0.3%)
        spread = float(mid_price) * random.uniform(0.001, 0.003)
        
        best_bid = mid_price - Decimal(str(spread / 2))
        best_ask = mid_price + Decimal(str(spread / 2))
        
        # Generate 30 levels
        tick_size = Decimal('0.05')
        
        for i in range(30):
            # Bid side
            bid_price = best_bid - (tick_size * i)
            bid_qty = random.randint(75, 2000)
            bid_prices.append(bid_price)
            bid_quantities.append(bid_qty)
            
            # Ask side
            ask_price = best_ask + (tick_size * i)
            ask_qty = random.randint(75, 2000)
            ask_prices.append(ask_price)
            ask_quantities.append(ask_qty)
        
        return bid_prices, bid_quantities, ask_prices, ask_quantities
    
    def _generate_greeks(self, price: Decimal) -> dict:
        """
        Generate option Greeks
        
        Args:
            price: Current price
            
        Returns:
            Dictionary with Greeks
        """
        # Simple Greeks simulation
        delta = 0.45 + random.gauss(0, 0.05)
        gamma = 0.0007 + random.gauss(0, 0.0002)
        theta = -17.5 + random.gauss(0, 2.0)
        vega = 12.5 + random.gauss(0, 1.5)
        rho = 1.85 + random.gauss(0, 0.3)
        iv = 0.17 + random.gauss(0, 0.02)
        
        return {
            'delta': max(0, min(1, delta)),
            'gamma': max(0, gamma),
            'theta': theta,
            'vega': max(0, vega),
            'rho': rho,
            'iv': max(0, iv)
        }
    
    def _generate_tick(self) -> TickReceivedEvent:
        """
        Generate a single tick event
        
        Returns:
            TickReceivedEvent
        """
        # Update price
        self.current_price = self._generate_price_movement()
        
        # Update volume (random increase)
        self.volume += random.randint(50, 500)
        
        # Update OI occasionally
        if random.random() < 0.1:  # 10% chance
            oi_change = random.randint(-5000, 5000)
            self.oi += oi_change
        
        # Generate order book
        bid_prices, bid_quantities, ask_prices, ask_quantities = \
            self._generate_order_book(self.current_price)
        
        # Generate Greeks
        greeks = self._generate_greeks(self.current_price)
        
        # TBQ/TSQ
        tbq = sum(bid_quantities)
        tsq = sum(ask_quantities)
        
        # Current time
        current_time = now_ist()
        candle_time = candle_minute(current_time)
        
        # Create tick event
        tick = TickReceivedEvent(
            instrument_key=self.instrument_key,
            raw_timestamp=str(int(current_time.timestamp() * 1000)),
            timestamp=current_time,
            candle_time=candle_time,
            
            # Price & volume
            ltp=self.current_price,
            ltq=random.randint(25, 150),
            volume=self.volume,
            oi=self.oi,
            atp=Decimal(str(self.current_price * 0.98)),
            previous_close=Decimal(str(self.base_price)),
            
            # Order book
            bid_prices=bid_prices,
            bid_quantities=bid_quantities,
            ask_prices=ask_prices,
            ask_quantities=ask_quantities,
            tbq=tbq,
            tsq=tsq,
            
            # Greeks
            delta=greeks['delta'],
            gamma=greeks['gamma'],
            theta=greeks['theta'],
            vega=greeks['vega'],
            rho=greeks['rho'],
            iv=greeks['iv']
        )
        
        self.tick_count += 1
        return tick
    
    async def start(self):
        """Start producing ticks"""
        if not self.event_bus:
            raise ValueError("Event bus not configured")
        
        await self.event_bus.connect()
        
        logger.info(f"🚀 Mock producer started for {self.instrument_key}")
        logger.info(f"   Base price: {self.base_price}")
        logger.info(f"   Tick interval: {self.tick_interval}s")
        
        self._running = True
        
        try:
            while self._running:
                # Check if market hours (optional)
                current_time = now_ist()
                if not is_trading_time(current_time):
                    logger.info("⏸️  Outside market hours, pausing...")
                    await asyncio.sleep(60)
                    continue
                
                # Generate and publish tick
                tick = self._generate_tick()
                await self.event_bus.publish(tick, "ticks")
                
                if self.tick_count % 50 == 0:
                    logger.info(
                        f"📊 Tick #{self.tick_count}: "
                        f"LTP={tick.ltp}, Volume={tick.volume:,}, OI={tick.oi:,}"
                    )
                
                # Wait for next tick
                await asyncio.sleep(self.tick_interval)
        
        except asyncio.CancelledError:
            logger.info("🛑 Mock producer stopped")
        except Exception as e:
            logger.error(f"❌ Error in mock producer: {e}", exc_info=True)
        finally:
            self._running = False
            await self.event_bus.disconnect()
    
    def stop(self):
        """Stop producing ticks"""
        self._running = False
        logger.info("🛑 Stopping mock producer...")


# ========================
# Testing
# ========================
if __name__ == "__main__":
    """
    Test mock producer
    Run: uv run python src/producers/mock_producer.py
    """
    
    async def test_mock_producer():
        print("=" * 70)
        print("Mock Tick Producer Test")
        print("=" * 70)
        print()
        
        # Create event bus
        bus = EventBus(redis_url=settings.get_redis_url)
        
        # Create mock producer
        producer = MockTickProducer(
            instrument_key="TEST_MOCK",
            base_price=182.00,
            tick_interval=0.5,  # 0.5 seconds for testing
            volatility=0.005,  # 0.5% volatility
            event_bus=bus
        )
        
        # Create consumer to verify
        received_ticks = []
        
        async def handle_tick(tick: TickReceivedEvent):
            received_ticks.append(tick)
            print(f"   ✅ Received: LTP={tick.ltp}, TBQ={tick.tbq:,}, TSQ={tick.tsq:,}")
        
        # Start consumer
        consumer_task = asyncio.create_task(
            bus.subscribe(
                stream_name="ticks",
                consumer_group="test_group",
                consumer_name="test_consumer",
                handler=handle_tick,
                event_type=TickReceivedEvent
            )
        )
        
        # Start producer
        producer_task = asyncio.create_task(producer.start())
        
        # Run for 10 seconds
        print("Running for 10 seconds...")
        print()
        await asyncio.sleep(10)
        
        # Stop
        producer.stop()
        bus.stop()
        
        await asyncio.sleep(1)
        
        producer_task.cancel()
        consumer_task.cancel()
        
        print()
        print("=" * 70)
        print(f"✅ Test complete! Generated {len(received_ticks)} ticks")
        print("=" * 70)
    
    asyncio.run(test_mock_producer())