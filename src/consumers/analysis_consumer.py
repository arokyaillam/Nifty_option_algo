"""
Analysis Consumer
Consumes candles and generates trading signals using seller state detection
"""

import asyncio
from datetime import datetime
from decimal import Decimal
from typing import Optional
import logging

import sys
from pathlib import Path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from src.event_bus.bus import EventBus
from src.events.candle_events import CandleCompletedEvent
from src.events.signal_events import SignalGeneratedEvent
from src.analysis.seller_detector import (
    SellerStateDetector,
    SellerState,
    Recommendation
)
from src.config.settings import settings

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class AnalysisConsumer:
    """
    Analyze candles and generate trading signals
    
    Flow:
    1. Subscribe to "candles" stream
    2. Run seller state detection on each candle
    3. Generate SignalGeneratedEvent
    4. Publish to "signals" stream
    """
    
    def __init__(self, event_bus: EventBus):
        """
        Initialize analysis consumer
        
        Args:
            event_bus: Event bus instance
        """
        self.event_bus = event_bus
        
        # Seller state detector
        self.detector = SellerStateDetector(
            oi_decrease_threshold=-0.003,     # -0.3% OI decrease
            price_increase_threshold=0.005,    # 0.5% price increase
            gamma_spike_threshold=0.30,        # 30% gamma spike
            order_book_panic_threshold=0.35,   # Order book ratio < 0.35
            spread_threshold=0.005,            # 0.5% spread
            vwap_deviation_threshold=0.01,     # 1% above VWAP
            panic_score_buy_threshold=60.0     # Panic score ≥ 60 → BUY
        )
        
        # Track previous candles for comparison
        self.previous_candles = {}
        
        self._running = False
        self.signal_count = 0
    
    async def _handle_candle(self, candle: CandleCompletedEvent):
        """
        Analyze candle and generate signal
        
        Args:
            candle: Completed candle event
        """
        try:
            # Get previous candle for OI change calculation
            prev_candle = self.previous_candles.get(candle.instrument_key)
            
            # Calculate price change
            price_change_pct = None
            if candle.previous_close and candle.previous_close > 0:
                price_change_pct = (candle.close - candle.previous_close) / candle.previous_close
            
            # Run seller state detection
            detection = self.detector.detect(
                # OI data
                oi_change_pct=candle.oi_change_pct,
                
                # Price data
                price=candle.close,
                previous_close=candle.previous_close,
                vwap=candle.vwap,
                
                # Greeks
                gamma_spike=candle.gamma_spike,
                
                # Order book
                order_book_ratio=candle.order_book_ratio,
                bid_ask_spread=candle.bid_ask_spread
            )
            
            # Log detection
            self._log_detection(candle, detection)
            
            # Create signal event
            signal = SignalGeneratedEvent(
                instrument_key=candle.instrument_key,
                candle_timestamp=candle.candle_timestamp,
                signal_timestamp=datetime.now(),
                
                # Signal details
                seller_state=detection.state,
                recommendation=detection.recommendation,
                confidence=detection.confidence,
                panic_score=detection.panic_score,
                
                # Price context
                entry_price=candle.close,
                support=candle.support,
                resistance=candle.resistance,
                
                # Candle metrics
                candle_score=candle.candle_score,
                
                # Flags
                short_covering=detection.short_covering,
                gamma_spike_detected=detection.gamma_spike_detected,
                order_book_panic=detection.order_book_panic,
                liquidity_drying=detection.liquidity_drying,
                strong_buying=detection.strong_buying,
                
                # Analysis details
                signals=detection.signals,
                
                # OI context
                oi_change=candle.oi_change,
                oi_change_pct=candle.oi_change_pct
            )
            
            # Publish signal
            await self.event_bus.publish(signal, "signals")
            
            self.signal_count += 1
            
            # Store candle for next comparison
            self.previous_candles[candle.instrument_key] = candle
        
        except Exception as e:
            logger.error(f"❌ Error analyzing candle: {e}", exc_info=True)
    
    def _log_detection(self, candle: CandleCompletedEvent, detection):
        """Log detection results"""
        
        # Color based on recommendation
        if detection.recommendation == Recommendation.BUY:
            icon = "🚨"
            color = "BUY"
        elif detection.recommendation == Recommendation.SELL:
            icon = "⚠️"
            color = "SELL"
        else:
            icon = "⏸️"
            color = "WAIT"
        
        # Build log message
        log_msg = (
            f"{icon} Signal #{self.signal_count + 1} | "
            f"{candle.instrument_key} @ {candle.candle_timestamp.strftime('%H:%M')} | "
            f"State: {detection.state} | "
            f"Recommendation: {color} | "
            f"Panic: {detection.panic_score}/100 | "
            f"Confidence: {detection.confidence * 100:.0f}%"
        )
        
        # Log with appropriate level
        if detection.recommendation == Recommendation.BUY:
            logger.warning(log_msg)  # Warning level for visibility
            
            # Log signals
            if detection.signals:
                logger.warning(f"   Signals: {', '.join(detection.signals)}")
            
            # Log key metrics
            logger.warning(
                f"   Price: {candle.close} | "
                f"OI Change: {candle.oi_change_pct * 100 if candle.oi_change_pct else 0:.2f}% | "
                f"Score: {candle.candle_score:.2f}"
            )
        else:
            logger.info(log_msg)
    
    async def start(self):
        """Start analysis consumer"""
        await self.event_bus.connect()
        
        logger.info("🚀 Analysis consumer started")
        logger.info("   Subscribing to 'candles' stream...")
        logger.info(f"   BUY threshold: Panic score ≥ {self.detector.panic_score_buy_threshold}")
        
        self._running = True
        
        # Subscribe to candles
        await self.event_bus.subscribe(
            stream_name="candles",
            consumer_group="analyzers",
            consumer_name="analyzer_1",
            handler=self._handle_candle,
            event_type=CandleCompletedEvent
        )
    
    def stop(self):
        """Stop analysis consumer"""
        self._running = False
        logger.info("🛑 Stopping analysis consumer...")


# ========================
# Testing
# ========================
if __name__ == "__main__":
    """
    Test analysis consumer
    
    Prerequisites:
    1. Producer running (generating ticks)
    2. Candle builder running (generating candles)
    
    Run: uv run python src/consumers/analysis_consumer.py
    """
    
    async def test_analysis_consumer():
        print("=" * 70)
        print("Analysis Consumer Test")
        print("=" * 70)
        print()
        
        print("Prerequisites:")
        print("  1. ✅ Producer must be running (ticks)")
        print("  2. ✅ Candle builder must be running (candles)")
        print()
        print("This consumer will:")
        print("  • Subscribe to 'candles' stream")
        print("  • Analyze seller behavior")
        print("  • Generate BUY/WAIT signals")
        print("  • Publish to 'signals' stream")
        print()
        print("Press Ctrl+C to stop")
        print()
        
        bus = EventBus(redis_url=settings.get_redis_url)
        consumer = AnalysisConsumer(event_bus=bus)
        
        await consumer.start()
    
    asyncio.run(test_analysis_consumer())