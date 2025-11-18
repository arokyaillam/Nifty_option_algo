"""
Mock Tick Producer - Fixed
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
from src.utils.timezone import now_ist, candle_minute
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class MockTickProducer:
    """Generate mock tick data for testing"""
    
    def __init__(
        self,
        instrument_key: str = "NSE_FO|61755",
        base_price: float = 182.00,
        tick_interval: float = 0.1,
        volatility: float = 0.002,
        event_bus: EventBus = None
    ):
        self.instrument_key = instrument_key
        self.base_price = base_price
        self.current_price = base_price  # Keep as float for calculations
        self.tick_interval = tick_interval
        self.volatility = volatility
        self.event_bus = event_bus
        
        self.volume = 0
        self.oi = 8326800
        self.tick_count = 0
        self._running = False
    
    def _generate_price_movement(self) -> float:
        """Generate next price"""
        change_pct = random.gauss(0, self.volatility)
        mean_reversion = (self.base_price - self.current_price) * 0.01
        change = self.current_price * change_pct + mean_reversion
        new_price = self.current_price + change
        new_price = max(new_price, self.base_price * 0.7)
        new_price = min(new_price, self.base_price * 1.3)
        return round(new_price, 2)
    
    def _generate_order_book(self, mid_price: float) -> tuple:
        """Generate 30-level order book"""
        bid_prices = []
        bid_quantities = []
        ask_prices = []
        ask_quantities = []
        
        spread = mid_price * random.uniform(0.001, 0.003)
        best_bid = mid_price - (spread / 2)
        best_ask = mid_price + (spread / 2)
        tick_size = 0.05
        
        for i in range(30):
            bid_prices.append(Decimal(str(round(best_bid - (tick_size * i), 2))))
            bid_quantities.append(random.randint(75, 2000))
            ask_prices.append(Decimal(str(round(best_ask + (tick_size * i), 2))))
            ask_quantities.append(random.randint(75, 2000))
        
        return bid_prices, bid_quantities, ask_prices, ask_quantities
    
    def _generate_greeks(self) -> dict:
        """Generate Greeks"""
        return {
            'delta': max(0, min(1, 0.45 + random.gauss(0, 0.05))),
            'gamma': max(0, 0.0007 + random.gauss(0, 0.0002)),
            'theta': -17.5 + random.gauss(0, 2.0),
            'vega': max(0, 12.5 + random.gauss(0, 1.5)),
            'rho': 1.85 + random.gauss(0, 0.3),
            'iv': max(0, 0.17 + random.gauss(0, 0.02))
        }
    
    def _generate_tick(self) -> TickReceivedEvent:
        """Generate tick"""
        # Update price
        self.current_price = self._generate_price_movement()
        self.volume += random.randint(50, 500)
        
        if random.random() < 0.1:
            self.oi += random.randint(-5000, 5000)
        
        # Order book
        bid_prices, bid_quantities, ask_prices, ask_quantities = \
            self._generate_order_book(self.current_price)
        
        greeks = self._generate_greeks()
        tbq = sum(bid_quantities)
        tsq = sum(ask_quantities)
        
        current_time = now_ist()
        candle_time = candle_minute(current_time)
        
        return TickReceivedEvent(
            instrument_key=self.instrument_key,
            raw_timestamp=str(int(current_time.timestamp() * 1000)),
            timestamp=current_time,
            candle_time=candle_time,
            ltp=Decimal(str(self.current_price)),
            ltq=random.randint(25, 150),
            volume=self.volume,
            oi=self.oi,
            atp=Decimal(str(round(self.current_price * 0.98, 2))),  # Fixed
            previous_close=Decimal(str(self.base_price)),
            bid_prices=bid_prices,
            bid_quantities=bid_quantities,
            ask_prices=ask_prices,
            ask_quantities=ask_quantities,
            tbq=tbq,
            tsq=tsq,
            delta=greeks['delta'],
            gamma=greeks['gamma'],
            theta=greeks['theta'],
            vega=greeks['vega'],
            rho=greeks['rho'],
            iv=greeks['iv']
        )
        self.tick_count += 1
    
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
                tick = self._generate_tick()
                await self.event_bus.publish(tick, "ticks")
                
                if self.tick_count % 50 == 0:
                    logger.info(
                        f"📊 Tick #{self.tick_count}: "
                        f"LTP={tick.ltp}, Volume={tick.volume:,}, OI={tick.oi:,}"
                    )
                
                await asyncio.sleep(self.tick_interval)
        
        except asyncio.CancelledError:
            logger.info("🛑 Mock producer stopped")
        except Exception as e:
            logger.error(f"❌ Error: {e}", exc_info=True)
        finally:
            self._running = False
            await self.event_bus.disconnect()
    
    def stop(self):
        """Stop producing"""
        self._running = False


if __name__ == "__main__":
    async def test():
        bus = EventBus(redis_url=settings.get_redis_url)
        producer = MockTickProducer(event_bus=bus)
        await producer.start()
    
    asyncio.run(test())