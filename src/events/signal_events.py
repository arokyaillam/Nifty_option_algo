"""
Signal Events
Trading signal generation and execution events
"""

from datetime import datetime
from decimal import Decimal
from typing import List, Optional, Literal
from pydantic import Field

from .base import BaseEvent


class SignalGeneratedEvent(BaseEvent):
    """
    Signal Generated Event
    Emitted when analysis detects a trading opportunity
    """
    
    event_type: Literal["signal_generated"] = "signal_generated"
    
    # Instrument & timing
    instrument_key: str
    candle_timestamp: datetime
    signal_timestamp: datetime
    
    # Signal details
    seller_state: str
    recommendation: str
    confidence: Decimal
    panic_score: Decimal
    
    # Price context
    entry_price: Decimal
    support: Optional[Decimal] = None
    resistance: Optional[Decimal] = None
    
    # Candle metrics
    candle_score: Decimal
    
    # Detection flags
    short_covering: bool = False
    gamma_spike_detected: bool = False
    order_book_panic: bool = False
    liquidity_drying: bool = False
    strong_buying: bool = False
    
    # Analysis details
    signals: List[str] = Field(default_factory=list)
    
    # OI context
    oi_change: Optional[Decimal] = None
    oi_change_pct: Optional[Decimal] = None
    
    # Optional
    stop_loss: Optional[Decimal] = None
    target_price: Optional[Decimal] = None
    
    class Config:
        json_encoders = {
            Decimal: lambda v: float(v) if v is not None else None,
            datetime: lambda v: v.isoformat()
        }


class SignalExecutedEvent(BaseEvent):
    """Signal Executed Event"""
    
    event_type: Literal["signal_executed"] = "signal_executed"
    
    signal_id: str
    instrument_key: str
    executed_at: datetime
    executed_price: Decimal
    quantity: int
    order_id: str
    order_type: str
    side: str
    
    class Config:
        json_encoders = {
            Decimal: float,
            datetime: lambda v: v.isoformat()
        }


class SignalClosedEvent(BaseEvent):
    """Signal Closed Event"""
    
    event_type: Literal["signal_closed"] = "signal_closed"
    
    signal_id: str
    instrument_key: str
    entry_price: Decimal
    entry_time: datetime
    exit_price: Decimal
    exit_time: datetime
    exit_reason: str
    pnl: Decimal
    pnl_pct: Decimal
    
    class Config:
        json_encoders = {
            Decimal: float,
            datetime: lambda v: v.isoformat()
        }