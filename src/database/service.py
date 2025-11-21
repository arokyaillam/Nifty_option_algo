"""
Database Service
CRUD operations for candles, signals, and seller states
"""

from datetime import datetime
from decimal import Decimal
from typing import List, Optional
from sqlalchemy import select, and_, desc
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.exc import IntegrityError
import logging

import sys
from pathlib import Path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from src.database.models import (
    Instrument,
    Candle,
    SellerState,
    Signal
)
from src.events.candle_events import CandleCompletedEvent
from src.events.signal_events import SignalGeneratedEvent

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DatabaseService:
    """
    Database operations service
    
    Features:
    - Save candles
    - Save signals
    - Save seller states
    - Query operations
    """
    
    def __init__(self, session: AsyncSession):
        """
        Initialize service
        
        Args:
            session: AsyncSession instance
        """
        self.session = session
    
    async def save_candle(self, candle_event: CandleCompletedEvent) -> Optional[Candle]:
        """
        Save candle to database
        
        Args:
            candle_event: CandleCompletedEvent
            
        Returns:
            Saved Candle or None if duplicate
        """
        try:
            candle = Candle(
                instrument_key=candle_event.instrument_key,
                candle_timestamp=candle_event.candle_timestamp,
                
                # OHLC
                open=candle_event.open,
                high=candle_event.high,
                low=candle_event.low,
                close=candle_event.close,
                previous_close=candle_event.previous_close,
                
                # Volume & OI
                volume=candle_event.volume,
                oi=candle_event.oi,
                oi_change=candle_event.oi_change,
                oi_change_pct=candle_event.oi_change_pct,
                
                # Metrics
                vwap=candle_event.vwap,
                price_vwap_deviation=candle_event.price_vwap_deviation,
                
                # Support levels
                support_level_1=candle_event.support_level_1,
                support_qty_1=candle_event.support_qty_1,
                support_level_2=candle_event.support_level_2,
                support_qty_2=candle_event.support_qty_2,
                support_level_3=candle_event.support_level_3,
                support_qty_3=candle_event.support_qty_3,
                support=candle_event.support,
                
                # Resistance levels
                resistance_level_1=candle_event.resistance_level_1,
                resistance_qty_1=candle_event.resistance_qty_1,
                resistance_level_2=candle_event.resistance_level_2,
                resistance_qty_2=candle_event.resistance_qty_2,
                resistance_level_3=candle_event.resistance_level_3,
                resistance_qty_3=candle_event.resistance_qty_3,
                resistance=candle_event.resistance,
                
                # Order book
                tbq=candle_event.tbq,
                tsq=candle_event.tsq,
                order_book_ratio=candle_event.order_book_ratio,
                bid_ask_spread=candle_event.bid_ask_spread,
                big_bid_count=candle_event.big_bid_count,
                big_ask_count=candle_event.big_ask_count,
                
                # Greeks
                avg_delta=candle_event.avg_delta,
                avg_gamma=candle_event.avg_gamma,
                avg_theta=candle_event.avg_theta,
                avg_vega=candle_event.avg_vega,
                avg_rho=candle_event.avg_rho,
                avg_iv=candle_event.avg_iv,
                gamma_spike=candle_event.gamma_spike,
                
                # Score
                candle_score=candle_event.candle_score,
                
                # Metadata
                tick_count=candle_event.tick_count
            )
            
            self.session.add(candle)
            await self.session.commit()
            await self.session.refresh(candle)
            
            logger.info(
                f"💾 Saved candle: {candle.instrument_key} "
                f"@ {candle.candle_timestamp.strftime('%H:%M')}"
            )
            
            return candle
        
        except IntegrityError:
            await self.session.rollback()
            logger.warning(
                f"⚠️  Duplicate candle: {candle_event.instrument_key} "
                f"@ {candle_event.candle_timestamp}"
            )
            return None
        
        except Exception as e:
            await self.session.rollback()
            logger.error(f"❌ Error saving candle: {e}", exc_info=True)
            return None
    
    async def save_signal(self, signal_event: SignalGeneratedEvent) -> Optional[Signal]:
        """
        Save signal to database
        
        Args:
            signal_event: SignalGeneratedEvent
            
        Returns:
            Saved Signal or None if error
        """
        try:
            signal = Signal(
                instrument_key=signal_event.instrument_key,
                candle_timestamp=signal_event.candle_timestamp,
                signal_timestamp=signal_event.signal_timestamp,
                
                # Signal details
                seller_state=signal_event.seller_state,
                recommendation=signal_event.recommendation,
                confidence=signal_event.confidence,
                panic_score=signal_event.panic_score,
                
                # Price context
                entry_price=signal_event.entry_price,
                support=signal_event.support,
                resistance=signal_event.resistance,
                
                # Metrics
                candle_score=signal_event.candle_score,
                
                # Flags
                short_covering=signal_event.short_covering,
                gamma_spike_detected=signal_event.gamma_spike_detected,
                order_book_panic=signal_event.order_book_panic,
                liquidity_drying=signal_event.liquidity_drying,
                strong_buying=signal_event.strong_buying,
                
                # OI
                oi_change=signal_event.oi_change,
                oi_change_pct=signal_event.oi_change_pct
            )
            
            self.session.add(signal)
            await self.session.commit()
            await self.session.refresh(signal)
            
            # Log with appropriate level
            if signal.recommendation == "BUY":
                logger.warning(
                    f"💾🚨 Saved BUY signal: {signal.instrument_key} "
                    f"@ {signal.candle_timestamp.strftime('%H:%M')} | "
                    f"Panic: {signal.panic_score}/100"
                )
            else:
                logger.info(
                    f"💾 Saved signal: {signal.instrument_key} "
                    f"@ {signal.candle_timestamp.strftime('%H:%M')}"
                )
            
            return signal
        
        except IntegrityError:
            await self.session.rollback()
            logger.warning(f"⚠️  Duplicate signal")
            return None
        
        except Exception as e:
            await self.session.rollback()
            logger.error(f"❌ Error saving signal: {e}", exc_info=True)
            return None
    
    async def save_seller_state(
        self,
        instrument_key: str,
        timestamp: datetime,
        state: str,
        panic_score: Decimal,
        confidence: Decimal
    ) -> Optional[SellerState]:
        """
        Save seller state snapshot
        
        Args:
            instrument_key: Instrument key
            timestamp: Timestamp
            state: Seller state
            panic_score: Panic score
            confidence: Confidence level
            
        Returns:
            Saved SellerState or None
        """
        try:
            seller_state = SellerState(
                instrument_key=instrument_key,
                timestamp=timestamp,
                state=state,
                panic_score=panic_score,
                confidence=confidence
            )
            
            self.session.add(seller_state)
            await self.session.commit()
            await self.session.refresh(seller_state)
            
            return seller_state
        
        except Exception as e:
            await self.session.rollback()
            logger.error(f"❌ Error saving seller state: {e}")
            return None
    
    async def get_latest_candles(
        self,
        instrument_key: str,
        limit: int = 10
    ) -> List[Candle]:
        """
        Get latest candles for instrument
        
        Args:
            instrument_key: Instrument key
            limit: Number of candles
            
        Returns:
            List of Candles
        """
        try:
            stmt = (
                select(Candle)
                .where(Candle.instrument_key == instrument_key)
                .order_by(desc(Candle.candle_timestamp))
                .limit(limit)
            )
            
            result = await self.session.execute(stmt)
            return list(result.scalars().all())
        
        except Exception as e:
            logger.error(f"❌ Error fetching candles: {e}")
            return []
    
    async def get_buy_signals(
        self,
        instrument_key: Optional[str] = None,
        min_panic_score: float = 60.0,
        limit: int = 20
    ) -> List[Signal]:
        """
        Get BUY signals
        
        Args:
            instrument_key: Filter by instrument (optional)
            min_panic_score: Minimum panic score
            limit: Number of signals
            
        Returns:
            List of Signals
        """
        try:
            conditions = [
                Signal.recommendation == "BUY",
                Signal.panic_score >= min_panic_score
            ]
            
            if instrument_key:
                conditions.append(Signal.instrument_key == instrument_key)
            
            stmt = (
                select(Signal)
                .where(and_(*conditions))
                .order_by(desc(Signal.signal_timestamp))
                .limit(limit)
            )
            
            result = await self.session.execute(stmt)
            return list(result.scalars().all())
        
        except Exception as e:
            logger.error(f"❌ Error fetching signals: {e}")
            return []
    
    async def get_candle_count(self) -> int:
        """Get total candle count"""
        try:
            from sqlalchemy import func
            stmt = select(func.count(Candle.id))
            result = await self.session.execute(stmt)
            return result.scalar() or 0
        except Exception as e:
            logger.error(f"❌ Error getting count: {e}")
            return 0
    
    async def get_signal_count(self) -> int:
        """Get total signal count"""
        try:
            from sqlalchemy import func
            stmt = select(func.count(Signal.id))
            result = await self.session.execute(stmt)
            return result.scalar() or 0
        except Exception as e:
            logger.error(f"❌ Error getting count: {e}")
            return 0


# ========================
# Testing
# ========================
if __name__ == "__main__":
    """
    Test database service
    Run: uv run python src/database/service.py
    """
    
    import asyncio
    from src.database.engine import get_async_session
    
    async def test_service():
        print("=" * 70)
        print("Database Service Test")
        print("=" * 70)
        print()
        
        async for session in get_async_session():
            service = DatabaseService(session)
            
            # Get counts
            candle_count = await service.get_candle_count()
            signal_count = await service.get_signal_count()
            
            print(f"Database Status:")
            print(f"  Candles: {candle_count}")
            print(f"  Signals: {signal_count}")
            print()
            
            # Get latest candles
            candles = await service.get_latest_candles("NSE_FO|61755", limit=5)
            print(f"Latest {len(candles)} candles:")
            for c in candles:
                print(f"  {c.candle_timestamp.strftime('%Y-%m-%d %H:%M')} | "
                      f"OHLC: {c.open}/{c.high}/{c.low}/{c.close}")
            print()
            
            # Get BUY signals
            signals = await service.get_buy_signals(limit=5)
            print(f"Latest {len(signals)} BUY signals:")
            for s in signals:
                print(f"  {s.signal_timestamp.strftime('%Y-%m-%d %H:%M')} | "
                      f"Panic: {s.panic_score}/100 | Price: {s.entry_price}")
            print()
            
            print("=" * 70)
            break
    
    asyncio.run(test_service())