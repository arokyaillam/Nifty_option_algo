"""
Database Models - Fixed Column Names
"""

from sqlalchemy import (
    Column, Integer, String, Numeric, DateTime, Boolean,
    ForeignKey, Index, UniqueConstraint, Text
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from datetime import datetime

Base = declarative_base()


class Instrument(Base):
    """Instrument master table"""
    __tablename__ = "instruments"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    instrument_key = Column(String(50), unique=True, nullable=False, index=True)
    exchange = Column(String(10))
    symbol = Column(String(50))
    expiry = Column(DateTime(timezone=True), nullable=True)
    strike = Column(Numeric(10, 2), nullable=True)
    option_type = Column(String(2), nullable=True)  # CE/PE
    lot_size = Column(Integer)
    tick_size = Column(Numeric(10, 2))
    created_at = Column(DateTime(timezone=True), default=datetime.utcnow)


class Candle(Base):
    """1-minute candle data"""
    __tablename__ = "candles"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    instrument_key = Column(String(50), nullable=False, index=True)
    candle_timestamp = Column(DateTime(timezone=True), nullable=False, index=True)
    
    # OHLC
    open = Column(Numeric(10, 2), nullable=False)
    high = Column(Numeric(10, 2), nullable=False)
    low = Column(Numeric(10, 2), nullable=False)
    close = Column(Numeric(10, 2), nullable=False)
    previous_close = Column(Numeric(10, 2))
    
    # Volume & OI
    volume = Column(Integer)
    oi = Column(Integer)
    oi_change = Column(Integer)
    oi_change_pct = Column(Numeric(10, 6))
    
    # Metrics
    vwap = Column(Numeric(10, 2))
    price_vwap_deviation = Column(Numeric(10, 6))
    
    # Support levels
    support_level_1 = Column(Numeric(10, 2))
    support_qty_1 = Column(Integer)
    support_level_2 = Column(Numeric(10, 2))
    support_qty_2 = Column(Integer)
    support_level_3 = Column(Numeric(10, 2))
    support_qty_3 = Column(Integer)
    support = Column(Numeric(10, 2))
    
    # Resistance levels
    resistance_level_1 = Column(Numeric(10, 2))
    resistance_qty_1 = Column(Integer)
    resistance_level_2 = Column(Numeric(10, 2))
    resistance_qty_2 = Column(Integer)
    resistance_level_3 = Column(Numeric(10, 2))
    resistance_qty_3 = Column(Integer)
    resistance = Column(Numeric(10, 2))
    
    # Order book
    tbq = Column(Integer)
    tsq = Column(Integer)
    order_book_ratio = Column(Numeric(10, 6))
    bid_ask_spread = Column(Numeric(10, 6))
    big_bid_count = Column(Integer)
    big_ask_count = Column(Integer)
    
    # Greeks
    avg_delta = Column(Numeric(10, 6))
    avg_gamma = Column(Numeric(10, 6))
    avg_theta = Column(Numeric(10, 6))
    avg_vega = Column(Numeric(10, 6))
    avg_rho = Column(Numeric(10, 6))
    avg_iv = Column(Numeric(10, 6))
    gamma_spike = Column(Numeric(10, 6))
    
    # Score
    candle_score = Column(Numeric(10, 2))
    
    # Metadata
    tick_count = Column(Integer)
    created_at = Column(DateTime(timezone=True), default=datetime.utcnow)
    
    __table_args__ = (
        UniqueConstraint('instrument_key', 'candle_timestamp', name='uq_candle'),
        Index('ix_candle_instrument_timestamp', 'instrument_key', 'candle_timestamp'),
    )


class SellerState(Base):
    """Seller behavior state tracking"""
    __tablename__ = "seller_states"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    instrument_key = Column(String(50), nullable=False, index=True)
    timestamp = Column(DateTime(timezone=True), nullable=False, index=True)
    state = Column(String(50), nullable=False)
    panic_score = Column(Numeric(10, 2))
    confidence = Column(Numeric(10, 6))
    created_at = Column(DateTime(timezone=True), default=datetime.utcnow)
    
    __table_args__ = (
        Index('ix_seller_instrument_timestamp', 'instrument_key', 'timestamp'),
    )


class Signal(Base):
    """Trading signals"""
    __tablename__ = "signals"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    instrument_key = Column(String(50), nullable=False, index=True)
    candle_timestamp = Column(DateTime(timezone=True), nullable=False)
    signal_timestamp = Column(DateTime(timezone=True), nullable=False, index=True)
    
    # Signal details
    seller_state = Column(String(50))
    recommendation = Column(String(10), nullable=False, index=True)
    confidence = Column(Numeric(10, 6))
    panic_score = Column(Numeric(10, 2))
    
    # Price context
    entry_price = Column(Numeric(10, 2))
    support = Column(Numeric(10, 2))
    resistance = Column(Numeric(10, 2))
    
    # Metrics
    candle_score = Column(Numeric(10, 2))
    
    # Detection flags
    short_covering = Column(Boolean, default=False)
    gamma_spike_detected = Column(Boolean, default=False)
    order_book_panic = Column(Boolean, default=False)
    liquidity_drying = Column(Boolean, default=False)
    strong_buying = Column(Boolean, default=False)
    
    # OI
    oi_change = Column(Integer)
    oi_change_pct = Column(Numeric(10, 6))
    
    # Execution tracking (future use)
    executed = Column(Boolean, default=False)
    executed_at = Column(DateTime(timezone=True), nullable=True)
    executed_price = Column(Numeric(10, 2), nullable=True)
    
    created_at = Column(DateTime(timezone=True), default=datetime.utcnow)
    
    __table_args__ = (
        Index('ix_signal_instrument_timestamp', 'instrument_key', 'signal_timestamp'),
        Index('ix_signal_recommendation', 'recommendation'),
    )


class Trade(Base):
    """Trade execution records"""
    __tablename__ = "trades"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    signal_id = Column(Integer, ForeignKey('signals.id'))
    instrument_key = Column(String(50), nullable=False)
    
    # Entry
    entry_time = Column(DateTime(timezone=True), nullable=False)
    entry_price = Column(Numeric(10, 2), nullable=False)
    quantity = Column(Integer, nullable=False)
    
    # Exit
    exit_time = Column(DateTime(timezone=True))
    exit_price = Column(Numeric(10, 2))
    exit_reason = Column(String(50))
    
    # P&L
    pnl = Column(Numeric(10, 2))
    pnl_pct = Column(Numeric(10, 6))
    
    # Order IDs
    entry_order_id = Column(String(50))
    exit_order_id = Column(String(50))
    
    created_at = Column(DateTime(timezone=True), default=datetime.utcnow)
    
    __table_args__ = (
        Index('ix_trade_instrument', 'instrument_key'),
        Index('ix_trade_entry_time', 'entry_time'),
    )


class TickSnapshot(Base):
    """Tick data snapshots (for detailed analysis)"""
    __tablename__ = "tick_snapshots"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    instrument_key = Column(String(50), nullable=False, index=True)
    timestamp = Column(DateTime(timezone=True), nullable=False, index=True)
    
    ltp = Column(Numeric(10, 2))
    volume = Column(Integer)
    oi = Column(Integer)
    
    # Order book (top 3 levels)
    bid1 = Column(Numeric(10, 2))
    bid1_qty = Column(Integer)
    bid2 = Column(Numeric(10, 2))
    bid2_qty = Column(Integer)
    bid3 = Column(Numeric(10, 2))
    bid3_qty = Column(Integer)
    
    ask1 = Column(Numeric(10, 2))
    ask1_qty = Column(Integer)
    ask2 = Column(Numeric(10, 2))
    ask2_qty = Column(Integer)
    ask3 = Column(Numeric(10, 2))
    ask3_qty = Column(Integer)
    
    # Greeks
    delta = Column(Numeric(10, 6))
    gamma = Column(Numeric(10, 6))
    theta = Column(Numeric(10, 6))
    vega = Column(Numeric(10, 6))
    iv = Column(Numeric(10, 6))
    
    created_at = Column(DateTime(timezone=True), default=datetime.utcnow)
    
    __table_args__ = (
        Index('ix_tick_instrument_timestamp', 'instrument_key', 'timestamp'),
    )


class SystemLog(Base):
    """System events and errors"""
    __tablename__ = "system_logs"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    timestamp = Column(DateTime(timezone=True), default=datetime.utcnow, index=True)
    level = Column(String(10))  # INFO, WARNING, ERROR
    component = Column(String(50))  # producer, consumer, analyzer
    message = Column(Text)
    details = Column(Text, nullable=True)
    
    __table_args__ = (
        Index('ix_log_timestamp_level', 'timestamp', 'level'),
    )


class BacktestResult(Base):
    """Backtest performance results"""
    __tablename__ = "backtest_results"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    strategy_name = Column(String(100), nullable=False)
    start_date = Column(DateTime(timezone=True), nullable=False)
    end_date = Column(DateTime(timezone=True), nullable=False)
    
    # Performance metrics
    total_trades = Column(Integer)
    winning_trades = Column(Integer)
    losing_trades = Column(Integer)
    win_rate = Column(Numeric(10, 6))
    
    total_pnl = Column(Numeric(15, 2))
    avg_pnl_per_trade = Column(Numeric(10, 2))
    max_drawdown = Column(Numeric(10, 6))
    sharpe_ratio = Column(Numeric(10, 6))
    
    # Config
    config_json = Column(Text)
    
    created_at = Column(DateTime(timezone=True), default=datetime.utcnow)