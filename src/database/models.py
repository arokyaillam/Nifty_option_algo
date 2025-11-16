"""
SQLAlchemy Database Models
All tables for the trading system
Updated with Upstox data structure fields
"""

from datetime import datetime, date
from decimal import Decimal
from typing import Optional

from sqlalchemy import (
    String, Integer, Numeric, DateTime, Date, Boolean, 
    Text, JSON, Index, UniqueConstraint, ForeignKey
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship
from sqlalchemy.ext.asyncio import AsyncAttrs
from sqlalchemy.sql import func

# Handle both relative and absolute imports
try:
    from ..utils.timezone import now_utc
except ImportError:
    import sys
    from pathlib import Path
    project_root = Path(__file__).parent.parent.parent
    sys.path.insert(0, str(project_root))
    
    # For now, use simple UTC function
    def now_utc():
        from datetime import datetime, timezone
        return datetime.now(timezone.utc)


# ========================
# Base Model
# ========================
class Base(AsyncAttrs, DeclarativeBase):
    """Base class for all models"""
    pass


# ========================
# Instruments Table
# ========================
class Instrument(Base):
    """
    Stores Nifty options instruments
    Refreshed monthly with ATM ±2 strikes
    """
    __tablename__ = "instruments"
    
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    
    # Upstox instrument identifier
    instrument_key: Mapped[str] = mapped_column(String(50), unique=True, nullable=False, index=True)
    
    # Contract details
    symbol: Mapped[str] = mapped_column(String(20), nullable=False)  # "NIFTY", "BANKNIFTY"
    expiry_date: Mapped[date] = mapped_column(Date, nullable=False, index=True)
    strike_price: Mapped[Decimal] = mapped_column(Numeric(10, 2), nullable=False)
    option_type: Mapped[str] = mapped_column(String(2), nullable=False)  # "CE" or "PE"
    
    # Status
    is_active: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False, index=True)
    
    # ATM distance (-2, -1, 0, +1, +2)
    atm_distance: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    
    # Timestamps
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=now_utc, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), 
        default=now_utc, 
        onupdate=now_utc,
        nullable=False
    )
    
    # Indexes
    __table_args__ = (
        Index('idx_instruments_symbol_expiry', 'symbol', 'expiry_date'),
        Index('idx_instruments_active_atm', 'is_active', 'atm_distance'),
    )
    
    def __repr__(self):
        return f"<Instrument {self.symbol} {self.strike_price} {self.option_type}>"


# ========================
# Candles Table
# ========================
class Candle(Base):
    """
    1-minute OHLC candles with comprehensive metrics
    Includes order book analysis, Greeks, and scores
    Based on Upstox market feed structure
    """
    __tablename__ = "candles"
    
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    
    # Identification
    instrument_key: Mapped[str] = mapped_column(String(50), nullable=False, index=True)
    timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, index=True)
    
    # ========================
    # OHLC Data
    # ========================
    open: Mapped[Decimal] = mapped_column(Numeric(12, 2), nullable=False)
    high: Mapped[Decimal] = mapped_column(Numeric(12, 2), nullable=False)
    low: Mapped[Decimal] = mapped_column(Numeric(12, 2), nullable=False)
    close: Mapped[Decimal] = mapped_column(Numeric(12, 2), nullable=False)  # LTP
    
    # Previous close / Change price
    previous_close: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 2), nullable=True)
    change: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 2), nullable=True)  # close - previous_close
    change_pct: Mapped[Optional[Decimal]] = mapped_column(Numeric(8, 4), nullable=True)  # % change
    
    # ========================
    # Volume & OI
    # ========================
    volume: Mapped[int] = mapped_column(Integer, nullable=False)  # From vtt (volume traded total)
    oi: Mapped[int] = mapped_column(Integer, nullable=False)
    
    # OI Change Metrics
    oi_change: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 2), nullable=True)
    oi_change_pct: Mapped[Optional[Decimal]] = mapped_column(Numeric(8, 4), nullable=True)
    
    # ========================
    # Calculated Metrics
    # ========================
    vwap: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 2), nullable=True)
    atp: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 2), nullable=True)  # Average Traded Price from Upstox
    price_vwap_deviation: Mapped[Optional[Decimal]] = mapped_column(Numeric(8, 6), nullable=True)
    
    # ========================
    # Support Levels (Top 3 by quantity)
    # ========================
    support_level_1: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 2), nullable=True)
    support_qty_1: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    
    support_level_2: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 2), nullable=True)
    support_qty_2: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    
    support_level_3: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 2), nullable=True)
    support_qty_3: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    
    support: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 2), nullable=True)  # Average
    
    # ========================
    # Resistance Levels (Top 3 by quantity)
    # ========================
    resistance_level_1: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 2), nullable=True)
    resistance_qty_1: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    
    resistance_level_2: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 2), nullable=True)
    resistance_qty_2: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    
    resistance_level_3: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 2), nullable=True)
    resistance_qty_3: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    
    resistance: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 2), nullable=True)  # Average
    
    # ========================
    # Order Book Metrics (30-depth from bidAskQuote)
    # ========================
    tbq: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)  # Total Bid Quantity (from Upstox)
    tsq: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)  # Total Sell Quantity (from Upstox)
    order_book_ratio: Mapped[Optional[Decimal]] = mapped_column(Numeric(5, 4), nullable=True)  # TBQ/(TBQ+TSQ)
    
    bid_ask_spread: Mapped[Optional[Decimal]] = mapped_column(Numeric(8, 6), nullable=True)
    
    big_bid_count: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)  # Whale bids
    big_ask_count: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)  # Whale asks
    
    # ========================
    # Greeks (Averaged over minute from optionGreeks)
    # ========================
    avg_delta: Mapped[Optional[Decimal]] = mapped_column(Numeric(8, 6), nullable=True)
    avg_gamma: Mapped[Optional[Decimal]] = mapped_column(Numeric(8, 6), nullable=True)
    avg_theta: Mapped[Optional[Decimal]] = mapped_column(Numeric(8, 6), nullable=True)
    avg_vega: Mapped[Optional[Decimal]] = mapped_column(Numeric(8, 6), nullable=True)
    avg_rho: Mapped[Optional[Decimal]] = mapped_column(Numeric(8, 6), nullable=True)  # ✅ ADDED
    avg_iv: Mapped[Optional[Decimal]] = mapped_column(Numeric(6, 4), nullable=True)
    
    # Gamma spike detection
    gamma_spike: Mapped[Optional[Decimal]] = mapped_column(Numeric(8, 4), nullable=True)
    
    # ========================
    # Candle Score
    # ========================
    candle_score: Mapped[Optional[Decimal]] = mapped_column(Numeric(15, 2), nullable=True)
    
    # ========================
    # Metadata
    # ========================
    tick_count: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=now_utc, nullable=False)
    
    # ========================
    # Indexes and Constraints
    # ========================
    __table_args__ = (
        UniqueConstraint('instrument_key', 'timestamp', name='uq_candle_instrument_time'),
        Index('idx_candles_instrument_time', 'instrument_key', 'timestamp'),
        Index('idx_candles_timestamp', 'timestamp'),
        Index('idx_candles_score', 'instrument_key', 'candle_score'),
    )
    
    def __repr__(self):
        return f"<Candle {self.instrument_key} @ {self.timestamp}>"


# ========================
# Straddle Candles Table
# ========================
class StraddleCandle(Base):
    """
    Straddle (CE + PE) analysis for each strike
    Used for premium compression/expansion detection
    """
    __tablename__ = "straddle_candles"
    
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    
    timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, index=True)
    strike_price: Mapped[Decimal] = mapped_column(Numeric(10, 2), nullable=False)
    
    # Straddle = CE price + PE price
    straddle_price: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 2), nullable=True)
    straddle_vwap: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 2), nullable=True)
    straddle_deviation: Mapped[Optional[Decimal]] = mapped_column(Numeric(8, 6), nullable=True)
    
    # References to component candles
    ce_candle_id: Mapped[Optional[int]] = mapped_column(ForeignKey('candles.id'), nullable=True)
    pe_candle_id: Mapped[Optional[int]] = mapped_column(ForeignKey('candles.id'), nullable=True)
    
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=now_utc, nullable=False)
    
    # ========================
    # Relationships
    # ========================
    ce_candle: Mapped[Optional["Candle"]] = relationship(foreign_keys=[ce_candle_id])
    pe_candle: Mapped[Optional["Candle"]] = relationship(foreign_keys=[pe_candle_id])
    
    # ========================
    # Indexes and Constraints
    # ========================
    __table_args__ = (
        UniqueConstraint('timestamp', 'strike_price', name='uq_straddle_time_strike'),
        Index('idx_straddle_timestamp', 'timestamp'),
    )
    
    def __repr__(self):
        return f"<StraddleCandle {self.strike_price} @ {self.timestamp}>"


# ========================
# Daily Key Levels Table
# ========================
class DailyKeyLevel(Base):
    """
    Day's key support/resistance from highest score candle
    """
    __tablename__ = "daily_key_levels"
    
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    
    instrument_key: Mapped[str] = mapped_column(String(50), nullable=False)
    trading_date: Mapped[date] = mapped_column(Date, nullable=False, index=True)
    
    # Key levels from biggest candle
    spot_support: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 2), nullable=True)
    spot_resistance: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 2), nullable=True)
    
    # Reference to source candle
    reference_candle_id: Mapped[Optional[int]] = mapped_column(ForeignKey('candles.id'), nullable=True)
    reference_candle_timestamp: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    reference_candle_score: Mapped[Optional[Decimal]] = mapped_column(Numeric(15, 2), nullable=True)
    
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=now_utc, nullable=False)
    
    # ========================
    # Relationships
    # ========================
    reference_candle: Mapped[Optional["Candle"]] = relationship()
    
    # ========================
    # Indexes and Constraints
    # ========================
    __table_args__ = (
        UniqueConstraint('instrument_key', 'trading_date', name='uq_daily_level_instrument_date'),
        Index('idx_daily_levels_date', 'trading_date'),
    )
    
    def __repr__(self):
        return f"<DailyKeyLevel {self.instrument_key} {self.trading_date}>"


# ========================
# Seller States Table
# ========================
class SellerState(Base):
    """
    Detected seller behavior states (PANIC, PROFIT_BOOKING, etc.)
    """
    __tablename__ = "seller_states"
    
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    
    instrument_key: Mapped[str] = mapped_column(String(50), nullable=False, index=True)
    timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, index=True)
    
    # Detected state
    state: Mapped[str] = mapped_column(String(20), nullable=False)  # PANIC, PROFIT_BOOKING, DIRECTION, NEUTRAL
    confidence: Mapped[Optional[Decimal]] = mapped_column(Numeric(5, 4), nullable=True)  # 0.0 to 1.0
    panic_score: Mapped[Optional[Decimal]] = mapped_column(Numeric(8, 2), nullable=True)  # 0 to 100
    
    # Detected signals (JSON array)
    signals: Mapped[Optional[dict]] = mapped_column(JSON, nullable=True)
    
    # Recommendation
    recommendation: Mapped[Optional[str]] = mapped_column(String(10), nullable=True)  # BUY, SELL, WAIT
    
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=now_utc, nullable=False)
    
    # ========================
    # Indexes
    # ========================
    __table_args__ = (
        Index('idx_seller_state_instrument_time', 'instrument_key', 'timestamp'),
        Index('idx_seller_state_recommendation', 'recommendation'),
    )
    
    def __repr__(self):
        return f"<SellerState {self.state} {self.instrument_key} @ {self.timestamp}>"


# ========================
# Signals Table
# ========================
class Signal(Base):
    """
    Generated buy/sell signals
    """
    __tablename__ = "signals"
    
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    
    instrument_key: Mapped[str] = mapped_column(String(50), nullable=False, index=True)
    timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, index=True)
    
    # Signal details
    signal_type: Mapped[str] = mapped_column(String(20), nullable=False)  # BUY, SELL, HOLD
    signal_strength: Mapped[Optional[Decimal]] = mapped_column(Numeric(5, 2), nullable=True)  # 0-100
    
    # Trigger reasons (boolean flags)
    panic_detected: Mapped[Optional[bool]] = mapped_column(Boolean, nullable=True)
    gamma_spike: Mapped[Optional[Decimal]] = mapped_column(Numeric(8, 4), nullable=True)
    iv_rise: Mapped[Optional[Decimal]] = mapped_column(Numeric(6, 4), nullable=True)
    vwap_breach: Mapped[Optional[bool]] = mapped_column(Boolean, nullable=True)
    
    # Recommendation
    entry_price: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 2), nullable=True)
    suggested_quantity: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    
    # Status tracking
    status: Mapped[str] = mapped_column(
        String(20), 
        default='GENERATED',
        nullable=False,
        index=True
    )  # GENERATED, EXECUTED, IGNORED, EXPIRED
    
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=now_utc, nullable=False)
    
    # ========================
    # Indexes
    # ========================
    __table_args__ = (
        Index('idx_signals_instrument_time', 'instrument_key', 'timestamp'),
        Index('idx_signals_status', 'status'),
    )
    
    def __repr__(self):
        return f"<Signal {self.signal_type} {self.instrument_key} @ {self.timestamp}>"


# ========================
# Orders Table
# ========================
class Order(Base):
    """
    Order tracking with Upstox integration
    """
    __tablename__ = "orders"
    
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    
    # Link to signal
    signal_id: Mapped[Optional[int]] = mapped_column(ForeignKey('signals.id'), nullable=True)
    
    instrument_key: Mapped[str] = mapped_column(String(50), nullable=False, index=True)
    
    # Upstox order details
    upstox_order_id: Mapped[Optional[str]] = mapped_column(String(50), unique=True, nullable=True)
    order_type: Mapped[Optional[str]] = mapped_column(String(20), nullable=True)  # MARKET, LIMIT
    transaction_type: Mapped[str] = mapped_column(String(10), nullable=False)  # BUY, SELL
    
    quantity: Mapped[int] = mapped_column(Integer, nullable=False)
    price: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 2), nullable=True)
    
    # Status tracking
    status: Mapped[str] = mapped_column(
        String(20),
        default='PENDING',
        nullable=False,
        index=True
    )  # PENDING, PLACED, FILLED, REJECTED, CANCELLED
    
    filled_quantity: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    average_price: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 2), nullable=True)
    
    # Timestamps
    placed_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    filled_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    rejected_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    
    # Error handling
    rejection_reason: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=now_utc, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=now_utc,
        onupdate=now_utc,
        nullable=False
    )
    
    # ========================
    # Relationships
    # ========================
    signal: Mapped[Optional["Signal"]] = relationship()
    
    # ========================
    # Indexes
    # ========================
    __table_args__ = (
        Index('idx_orders_status', 'status'),
        Index('idx_orders_instrument', 'instrument_key'),
    )
    
    def __repr__(self):
        return f"<Order {self.transaction_type} {self.quantity} {self.instrument_key}>"


# ========================
# System Config Table
# ========================
class SystemConfig(Base):
    """
    System-wide configuration key-value store
    """
    __tablename__ = "system_config"
    
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    
    config_key: Mapped[str] = mapped_column(String(50), unique=True, nullable=False)
    config_value: Mapped[str] = mapped_column(Text, nullable=False)
    description: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=now_utc,
        onupdate=now_utc,
        nullable=False
    )
    
    def __repr__(self):
        return f"<SystemConfig {self.config_key}={self.config_value}>"


# ========================
# Helper Functions
# ========================
async def create_all_tables(engine):
    """
    Create all tables in database
    Use this for initial setup or testing
    
    Usage:
        from src.database.engine import engine
        from src.database.models import create_all_tables
        await create_all_tables(engine)
    """
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)


async def drop_all_tables(engine):
    """
    Drop all tables (BE CAREFUL!)
    Only use in testing
    """
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)


# ========================
# Testing
# ========================
if __name__ == "__main__":
    """
    Test models definition
    Run: uv run python src/database/models.py
    """
    import asyncio
    from sqlalchemy import inspect
    
    async def test_models():
        print("=" * 60)
        print("Database Models Test (Updated with Upstox Fields)")
        print("=" * 60)
        print()
        
        # Import engine
        try:
            from src.database.engine import create_engine
        except ImportError:
            from engine import create_engine
        
        engine = create_engine()
        
        # Get table names
        async with engine.begin() as conn:
            def get_tables(connection):
                inspector = inspect(connection)
                return Base.metadata.tables.keys()
            
            tables = await conn.run_sync(get_tables)
        
        print(f"✅ Defined {len(tables)} tables:")
        for i, table in enumerate(tables, 1):
            print(f"   {i}. {table}")
        
        print()
        
        # Show model details
        print("Model Details:")
        print("-" * 60)
        
        models = [
            Instrument, Candle, StraddleCandle, DailyKeyLevel,
            SellerState, Signal, Order, SystemConfig
        ]
        
        for model in models:
            column_count = len(model.__table__.columns)
            index_count = len(model.__table__.indexes)
            print(f"  {model.__name__:20} - {column_count:2} columns, {index_count:2} indexes")
        
        print()
        
        # Show added fields
        print("✅ New Fields Added to Candle Model:")
        print("   - avg_rho (Option Greek)")
        print("   - atp (Average Traded Price)")
        print("   - previous_close")
        print("   - change")
        print("   - change_pct")
        
        print()
        print("=" * 60)
        print("🎉 All models defined successfully!")
        print("=" * 60)
        
        await engine.dispose()
    
    asyncio.run(test_models())