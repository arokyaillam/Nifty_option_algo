"""
Panic Scenario Mock Producer
Generates realistic panic conditions for testing BUY signals
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


class PanicScenario:
    """Define different panic scenarios"""
    
    @staticmethod
    def short_covering():
        """Short covering: OI↓ + Price↑"""
        return {
            'price_trend': 0.015,      # 1.5% upward trend
            'oi_trend': -0.008,        # -0.8% OI decrease
            'gamma_spike': 0.55,       # 55% gamma spike
            'order_book_imbalance': 0.25,  # Heavy sell-side
            'spread_widening': 0.008,  # 0.8% spread
            'name': 'SHORT_COVERING'
        }
    
    @staticmethod
    def gamma_squeeze():
        """Massive gamma spike"""
        return {
            'price_trend': 0.012,
            'oi_trend': -0.005,
            'gamma_spike': 0.75,       # 75% gamma spike!
            'order_book_imbalance': 0.30,
            'spread_widening': 0.006,
            'name': 'GAMMA_SQUEEZE'
        }
    
    @staticmethod
    def liquidity_crisis():
        """Liquidity drying up"""
        return {
            'price_trend': 0.008,
            'oi_trend': -0.003,
            'gamma_spike': 0.35,
            'order_book_imbalance': 0.28,
            'spread_widening': 0.012,  # 1.2% spread!
            'name': 'LIQUIDITY_CRISIS'
        }
    
    @staticmethod
    def normal():
        """Normal market conditions"""
        return {
            'price_trend': 0.002,
            'oi_trend': 0.001,
            'gamma_spike': 0.10,
            'order_book_imbalance': 0.48,
            'spread_widening': 0.002,
            'name': 'NORMAL'
        }


class PanicMockProducer:
    """Mock producer with configurable panic scenarios"""
    
    def __init__(
        self,
        instrument_key: str = "NSE_FO|61755",
        base_price: float = 182.00,
        tick_interval: float = 0.1,
        event_bus: EventBus = None,
        panic_probability: float = 0.15  # 15% chance of panic per minute
    ):
        self.instrument_key = instrument_key
        self.base_price = base_price
        self.current_price = base_price
        self.tick_interval = tick_interval
        self.event_bus = event_bus
        self.panic_probability = panic_probability
        
        self.volume = 0
        self.oi = 8326800
        self.tick_count = 0
        self._running = False
        
        # Current scenario
        self.current_scenario = PanicScenario.normal()
        self.scenario_ticks_remaining = 0
        self.base_gamma = 0.0007
    
    def _maybe_switch_scenario(self):
        """Randomly switch to panic scenario"""
        if self.scenario_ticks_remaining > 0:
            self.scenario_ticks_remaining -= 1
            return
        
        # Check if should enter panic
        if random.random() < self.panic_probability:
            # Choose random panic scenario
            scenarios = [
                PanicScenario.short_covering(),
                PanicScenario.gamma_squeeze(),
                PanicScenario.liquidity_crisis()
            ]
            self.current_scenario = random.choice(scenarios)
            self.scenario_ticks_remaining = random.randint(300, 600)  # 30-60 seconds
            
            logger.warning(
                f"🚨 PANIC SCENARIO ACTIVATED: {self.current_scenario['name']} "
                f"(Duration: {self.scenario_ticks_remaining} ticks)"
            )
        else:
            # Return to normal
            if self.current_scenario['name'] != 'NORMAL':
                logger.info("✅ Returning to normal market conditions")
            self.current_scenario = PanicScenario.normal()
            self.scenario_ticks_remaining = random.randint(200, 400)
    
    def _generate_price(self) -> float:
        """Generate price with scenario bias"""
        base_change = random.gauss(0, 0.002)
        scenario_bias = self.current_scenario['price_trend'] / 100  # Per tick
        
        change = self.current_price * (base_change + scenario_bias)
        new_price = self.current_price + change
        
        new_price = max(new_price, self.base_price * 0.7)
        new_price = min(new_price, self.base_price * 1.3)
        
        return round(new_price, 2)
    
    def _generate_order_book(self, mid_price: float) -> tuple:
        """Generate order book with scenario imbalance"""
        bid_prices = []
        bid_quantities = []
        ask_prices = []
        ask_quantities = []
        
        # Spread based on scenario
        spread_pct = self.current_scenario['spread_widening']
        spread = mid_price * spread_pct
        
        best_bid = mid_price - (spread / 2)
        best_ask = mid_price + (spread / 2)
        tick_size = 0.05
        
        # Imbalance ratio (0.25 = ask-heavy = panic)
        imbalance = self.current_scenario['order_book_imbalance']
        
        for i in range(30):
            # Bid quantities (reduced in panic)
            if imbalance < 0.40:  # Panic scenario
                bid_qty = random.randint(50, 800)  # Less buyers
            else:
                bid_qty = random.randint(75, 2000)
            
            # Ask quantities (increased in panic)
            if imbalance < 0.40:  # Panic scenario
                ask_qty = random.randint(500, 3000)  # More sellers!
            else:
                ask_qty = random.randint(75, 2000)
            
            bid_prices.append(Decimal(str(round(best_bid - (tick_size * i), 2))))
            bid_quantities.append(bid_qty)
            ask_prices.append(Decimal(str(round(best_ask + (tick_size * i), 2))))
            ask_quantities.append(ask_qty)
        
        return bid_prices, bid_quantities, ask_prices, ask_quantities
    
    def _generate_greeks(self) -> dict:
        """Generate Greeks with gamma spike"""
        # Base gamma with scenario spike
        gamma = self.base_gamma * (1 + self.current_scenario['gamma_spike'])
        gamma += random.gauss(0, 0.0001)
        
        return {
            'delta': max(0, min(1, 0.45 + random.gauss(0, 0.05))),
            'gamma': max(0, gamma),
            'theta': -17.5 + random.gauss(0, 2.0),
            'vega': max(0, 12.5 + random.gauss(0, 1.5)),
            'rho': 1.85 + random.gauss(0, 0.3),
            'iv': max(0, 0.17 + random.gauss(0, 0.02))
        }
    
    def _generate_tick(self) -> TickReceivedEvent:
        """Generate tick with scenario"""
        self._maybe_switch_scenario()
        
        # Update price
        self.current_price = self._generate_price()
        self.volume += random.randint(50, 500)
        
        # OI change based on scenario
        if random.random() < 0.3:  # 30% chance
            oi_change_pct = self.current_scenario['oi_trend']
            oi_change = int(self.oi * oi_change_pct)
            self.oi += oi_change
        
        # Order book
        bid_prices, bid_quantities, ask_prices, ask_quantities = \
            self._generate_order_book(self.current_price)
        
        greeks = self._generate_greeks()
        tbq = sum(bid_quantities)
        tsq = sum(ask_quantities)
        
        current_time = now_ist()
        candle_time = candle_minute(current_time)
        
        tick = TickReceivedEvent(
            instrument_key=self.instrument_key,
            raw_timestamp=str(int(current_time.timestamp() * 1000)),
            timestamp=current_time,
            candle_time=candle_time,
            ltp=Decimal(str(self.current_price)),
            ltq=random.randint(25, 150),
            volume=self.volume,
            oi=self.oi,
            atp=Decimal(str(round(self.current_price * 0.98, 2))),
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
        return tick
    
    async def start(self):
        """Start producing"""
        if not self.event_bus:
            raise ValueError("Event bus not configured")
        
        await self.event_bus.connect()
        
        logger.info(f"🚀 Panic Mock Producer started")
        logger.info(f"   Instrument: {self.instrument_key}")
        logger.info(f"   Base price: {self.base_price}")
        logger.info(f"   Panic probability: {self.panic_probability * 100:.0f}%")
        logger.info(f"   🎯 Will generate BUY signals during panic scenarios!")
        
        self._running = True
        
        try:
            while self._running:
                tick = self._generate_tick()
                await self.event_bus.publish(tick, "ticks")
                
                if self.tick_count % 50 == 0:
                    logger.info(
                        f"📊 Tick #{self.tick_count}: "
                        f"Scenario={self.current_scenario['name']}, "
                        f"LTP={tick.ltp}, OI={tick.oi:,}"
                    )
                
                await asyncio.sleep(self.tick_interval)
        
        except asyncio.CancelledError:
            logger.info("🛑 Producer stopped")
        finally:
            self._running = False
            await self.event_bus.disconnect()
    
    def stop(self):
        self._running = False


if __name__ == "__main__":
    async def test():
        bus = EventBus(redis_url=settings.get_redis_url)
        producer = PanicMockProducer(
            event_bus=bus,
            panic_probability=0.30  # 30% chance for testing
        )
        await producer.start()
    
    asyncio.run(test())