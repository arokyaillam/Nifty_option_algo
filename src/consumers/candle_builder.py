"""
Candle Builder Consumer
Consumes ticks from Redis and builds 1-minute candles
"""

import asyncio
from datetime import datetime
from decimal import Decimal
from typing import Dict, List
from collections import defaultdict
import logging

import sys
from pathlib import Path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from src.event_bus.bus import EventBus
from src.events.tick_events import TickReceivedEvent
from src.events.candle_events import CandleCompletedEvent
from src.analysis.orderbook_analyzer import OrderBookAnalyzer
from src.analysis.candle_score import CandleScoreCalculator
from src.analysis.metrics_calculator import MetricsCalculator
from src.config.settings import settings

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class CandleData:
    """Accumulator for building candles from ticks"""
    
    def __init__(self, instrument_key: str, candle_time: datetime):
        self.instrument_key = instrument_key
        self.candle_time = candle_time
        
        # Price data
        self.open: Decimal = None
        self.high: Decimal = None
        self.low: Decimal = None
        self.close: Decimal = None
        self.previous_close: Decimal = None
        
        # Volume & OI
        self.volume = 0
        self.oi = 0
        self.oi_at_start = None
        
        # Order book snapshots (for averaging)
        self.bid_prices_list: List[List[Decimal]] = []
        self.bid_quantities_list: List[List[int]] = []
        self.ask_prices_list: List[List[Decimal]] = []
        self.ask_quantities_list: List[List[int]] = []
        
        # Greeks (for averaging)
        self.deltas: List[float] = []
        self.gammas: List[float] = []
        self.thetas: List[float] = []
        self.vegas: List[float] = []
        self.rhos: List[float] = []
        self.ivs: List[float] = []
        
        # Gamma spike detection
        self.first_gamma: float = None
        self.last_gamma: float = None
        
        # Metadata
        self.tick_count = 0
        self.first_tick_time: datetime = None
        self.last_tick_time: datetime = None
    
    def add_tick(self, tick: TickReceivedEvent):
        """Add tick to candle"""
        self.tick_count += 1
        
        # First tick
        if self.open is None:
            self.open = tick.ltp
            self.high = tick.ltp
            self.low = tick.ltp
            self.previous_close = tick.previous_close
            self.oi_at_start = tick.oi
            self.first_tick_time = tick.timestamp
            self.first_gamma = tick.gamma
        
        # Update OHLC
        self.close = tick.ltp
        self.high = max(self.high, tick.ltp)
        self.low = min(self.low, tick.ltp)
        
        # Update volume & OI
        self.volume = tick.volume  # Use latest volume (cumulative from Upstox)
        self.oi = tick.oi
        
        # Store order book snapshots
        if tick.bid_prices and tick.ask_prices:
            self.bid_prices_list.append(tick.bid_prices)
            self.bid_quantities_list.append(tick.bid_quantities)
            self.ask_prices_list.append(tick.ask_prices)
            self.ask_quantities_list.append(tick.ask_quantities)
        
        # Store Greeks
        if tick.delta is not None:
            self.deltas.append(tick.delta)
        if tick.gamma is not None:
            self.gammas.append(tick.gamma)
            self.last_gamma = tick.gamma
        if tick.theta is not None:
            self.thetas.append(tick.theta)
        if tick.vega is not None:
            self.vegas.append(tick.vega)
        if tick.rho is not None:
            self.rhos.append(tick.rho)
        if tick.iv is not None:
            self.ivs.append(tick.iv)
        
        self.last_tick_time = tick.timestamp


class CandleBuilder:
    """
    Build 1-minute candles from ticks
    
    Flow:
    1. Subscribe to "ticks" stream
    2. Group ticks by instrument_key + candle_time
    3. Accumulate OHLC, volume, OI
    4. On minute boundary, calculate metrics
    5. Publish CandleCompletedEvent to "candles" stream
    """
    
    def __init__(self, event_bus: EventBus):
        """
        Initialize candle builder
        
        Args:
            event_bus: Event bus instance
        """
        self.event_bus = event_bus
        
        # Active candles being built
        self.active_candles: Dict[tuple, CandleData] = {}
        
        # Previous candles (for OI change calculation)
        self.previous_candles: Dict[str, CandleData] = {}
        
        # Analyzers
        self.ob_analyzer = OrderBookAnalyzer()
        self.score_calculator = CandleScoreCalculator()
        self.metrics_calc = MetricsCalculator()
        
        self._running = False
        self._last_minute_check = None
    
    def _get_candle_key(self, instrument_key: str, candle_time: datetime) -> tuple:
        """Get unique key for candle"""
        return (instrument_key, candle_time)
    
    def _get_or_create_candle(
        self,
        instrument_key: str,
        candle_time: datetime
    ) -> CandleData:
        """Get existing or create new candle"""
        key = self._get_candle_key(instrument_key, candle_time)
        
        if key not in self.active_candles:
            self.active_candles[key] = CandleData(instrument_key, candle_time)
        
        return self.active_candles[key]
    
    def _calculate_order_book_metrics(self, candle: CandleData) -> dict:
        """Calculate order book metrics from snapshots"""
        if not candle.bid_prices_list:
            return {}
        
        # Use last snapshot for order book analysis
        last_bids = candle.bid_prices_list[-1]
        last_bid_qtys = candle.bid_quantities_list[-1]
        last_asks = candle.ask_prices_list[-1]
        last_ask_qtys = candle.ask_quantities_list[-1]
        
        # Analyze order book
        ob_metrics = self.ob_analyzer.analyze_order_book(
            last_bids, last_bid_qtys,
            last_asks, last_ask_qtys
        )
        
        return ob_metrics
    
    def _calculate_greek_averages(self, candle: CandleData) -> dict:
        """Calculate average Greeks"""
        return {
            'avg_delta': self.metrics_calc.calculate_average_greek(candle.deltas),
            'avg_gamma': self.metrics_calc.calculate_average_greek(candle.gammas),
            'avg_theta': self.metrics_calc.calculate_average_greek(candle.thetas),
            'avg_vega': self.metrics_calc.calculate_average_greek(candle.vegas),
            'avg_rho': self.metrics_calc.calculate_average_greek(candle.rhos),
            'avg_iv': self.metrics_calc.calculate_average_greek(candle.ivs),
        }
    
    def _calculate_gamma_spike(self, candle: CandleData) -> Decimal:
        """Calculate gamma spike"""
        if candle.first_gamma and candle.last_gamma:
            spike = self.metrics_calc.calculate_gamma_spike(
                candle.last_gamma,
                candle.first_gamma
            )
            return spike if spike else Decimal('0')
        return Decimal('0')
    
    def _build_candle_event(self, candle: CandleData) -> CandleCompletedEvent:
        """Build CandleCompletedEvent from accumulated data"""
        
        # Order book metrics
        ob_metrics = self._calculate_order_book_metrics(candle)
        
        # Greek averages
        greek_avgs = self._calculate_greek_averages(candle)
        
        # Gamma spike
        gamma_spike = self._calculate_gamma_spike(candle)
        
        # OI change (compare with previous candle)
        oi_change = None
        oi_change_pct = None
        prev_candle = self.previous_candles.get(candle.instrument_key)
        if prev_candle and prev_candle.oi > 0:
            oi_change, oi_change_pct = self.metrics_calc.calculate_oi_change(
                candle.oi,
                prev_candle.oi
            )
        
        # VWAP calculation (simple average of close prices weighted by volume)
        # For now, use close as approximation
        vwap = candle.close
        price_vwap_deviation = Decimal('0')
        
        # Calculate candle score
        candle_score = self.score_calculator.calculate_score(
            volume=candle.volume,
            oi_change=oi_change,
            oi_change_pct=oi_change_pct,
            order_book_ratio=ob_metrics.get('order_book_ratio'),
            high=candle.high,
            low=candle.low,
            close=candle.close,
            gamma_spike=gamma_spike,
            bid_ask_spread=ob_metrics.get('bid_ask_spread')
        )
        
        # Build event
        return CandleCompletedEvent(
            instrument_key=candle.instrument_key,
            candle_timestamp=candle.candle_time,
            
            # OHLC
            open=candle.open,
            high=candle.high,
            low=candle.low,
            close=candle.close,
            previous_close=candle.previous_close,
            
            # Volume & OI
            volume=candle.volume,
            oi=candle.oi,
            oi_change=oi_change,
            oi_change_pct=oi_change_pct,
            
            # Metrics
            vwap=vwap,
            price_vwap_deviation=price_vwap_deviation,
            
            # Support/Resistance
            support_level_1=ob_metrics.get('support_level_1'),
            support_qty_1=ob_metrics.get('support_qty_1'),
            support_level_2=ob_metrics.get('support_level_2'),
            support_qty_2=ob_metrics.get('support_qty_2'),
            support_level_3=ob_metrics.get('support_level_3'),
            support_qty_3=ob_metrics.get('support_qty_3'),
            support=ob_metrics.get('support'),
            
            resistance_level_1=ob_metrics.get('resistance_level_1'),
            resistance_qty_1=ob_metrics.get('resistance_qty_1'),
            resistance_level_2=ob_metrics.get('resistance_level_2'),
            resistance_qty_2=ob_metrics.get('resistance_qty_2'),
            resistance_level_3=ob_metrics.get('resistance_level_3'),
            resistance_qty_3=ob_metrics.get('resistance_qty_3'),
            resistance=ob_metrics.get('resistance'),
            
            # Order book
            tbq=ob_metrics.get('tbq'),
            tsq=ob_metrics.get('tsq'),
            order_book_ratio=ob_metrics.get('order_book_ratio'),
            bid_ask_spread=ob_metrics.get('bid_ask_spread'),
            big_bid_count=ob_metrics.get('big_bid_count'),
            big_ask_count=ob_metrics.get('big_ask_count'),
            
            # Greeks
            avg_delta=greek_avgs.get('avg_delta'),
            avg_gamma=greek_avgs.get('avg_gamma'),
            avg_theta=greek_avgs.get('avg_theta'),
            avg_vega=greek_avgs.get('avg_vega'),
            avg_rho=greek_avgs.get('avg_rho'),
            avg_iv=greek_avgs.get('avg_iv'),
            gamma_spike=gamma_spike,
            
            # Score
            candle_score=candle_score,
            
            # Metadata
            tick_count=candle.tick_count
        )
    
    async def _handle_tick(self, tick: TickReceivedEvent):
        """Handle incoming tick"""
        try:
            # Get or create candle
            candle = self._get_or_create_candle(
                tick.instrument_key,
                tick.candle_time
            )
            
            # Add tick to candle
            candle.add_tick(tick)
            
        except Exception as e:
            logger.error(f"❌ Error handling tick: {e}", exc_info=True)
    
    async def _check_and_complete_candles(self, current_time: datetime):
        """Check if any candles should be completed"""
        completed_keys = []
        
        for key, candle in self.active_candles.items():
            # If current time is in next minute, complete this candle
            if current_time.minute != candle.candle_time.minute:
                try:
                    # Build candle event
                    candle_event = self._build_candle_event(candle)
                    
                    # Publish to candles stream
                    await self.event_bus.publish(candle_event, "candles")
                    
                    logger.info(
                        f"🕯️  Candle complete: {candle.instrument_key} "
                        f"@ {candle.candle_time.strftime('%H:%M')} | "
                        f"OHLC: {candle.open}/{candle.high}/{candle.low}/{candle.close} | "
                        f"Ticks: {candle.tick_count} | Score: {candle_event.candle_score:.2f}"
                    )
                    
                    # Store as previous candle
                    self.previous_candles[candle.instrument_key] = candle
                    
                    # Mark for removal
                    completed_keys.append(key)
                
                except Exception as e:
                    logger.error(f"❌ Error completing candle: {e}", exc_info=True)
        
        # Remove completed candles
        for key in completed_keys:
            del self.active_candles[key]
    
    async def start(self):
        """Start candle builder"""
        await self.event_bus.connect()
        
        logger.info("🚀 Candle builder started")
        logger.info("   Subscribing to 'ticks' stream...")
        
        self._running = True
        
        # Subscribe to ticks
        async def tick_handler(tick: TickReceivedEvent):
            await self._handle_tick(tick)
            
            # Check for candle completion every minute
            if self._last_minute_check != tick.candle_time.minute:
                await self._check_and_complete_candles(tick.timestamp)
                self._last_minute_check = tick.candle_time.minute
        
        await self.event_bus.subscribe(
            stream_name="ticks",
            consumer_group="candle_builders",
            consumer_name="builder_1",
            handler=tick_handler,
            event_type=TickReceivedEvent
        )
    
    def stop(self):
        """Stop candle builder"""
        self._running = False
        logger.info("🛑 Stopping candle builder...")


# ========================
# Testing
# ========================
if __name__ == "__main__":
    """
    Test candle builder
    Run: uv run python src/consumers/candle_builder.py
    """
    
    async def test_candle_builder():
        print("=" * 70)
        print("Candle Builder Test")
        print("=" * 70)
        print()
        
        bus = EventBus(redis_url=settings.get_redis_url)
        builder = CandleBuilder(event_bus=bus)
        
        print("Starting candle builder...")
        print("Run mock producer in another terminal:")
        print("   uv run python scripts/run_producer_anytime.py")
        print()
        print("Press Ctrl+C to stop")
        print()
        
        await builder.start()
    
    asyncio.run(test_candle_builder())