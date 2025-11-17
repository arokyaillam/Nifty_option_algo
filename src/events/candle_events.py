"""
Candle Events
Events for completed 1-minute candles with all calculated metrics
"""

from datetime import datetime
from decimal import Decimal
from typing import Optional
from pydantic import Field

# Handle imports
try:
    from .base import BaseEvent
except ImportError:
    import sys
    from pathlib import Path
    project_root = Path(__file__).parent.parent.parent
    sys.path.insert(0, str(project_root))
    from src.events.base import BaseEvent


class CandleCompletedEvent(BaseEvent):
    """
    Event emitted when a 1-minute candle is completed
    
    Contains:
    - OHLC data
    - Volume & OI
    - Support/Resistance (3 levels each)
    - Order book metrics
    - Greeks (averaged)
    - Candle score
    """
    
    event_type: str = "candle.completed"
    
    # ========================
    # Identification
    # ========================
    instrument_key: str = Field(
        description="Instrument identifier"
    )
    
    candle_timestamp: datetime = Field(
        description="Candle start time (IST, minute boundary)"
    )
    
    # ========================
    # OHLC
    # ========================
    open: Decimal = Field(description="Open price")
    high: Decimal = Field(description="High price")
    low: Decimal = Field(description="Low price")
    close: Decimal = Field(description="Close price (LTP)")
    
    previous_close: Optional[Decimal] = Field(
        default=None,
        description="Previous close for change calculation"
    )
    
    # ========================
    # Volume & OI
    # ========================
    volume: int = Field(description="Volume")
    oi: int = Field(description="Open Interest")
    
    oi_change: Optional[Decimal] = Field(
        default=None,
        description="OI change from previous candle"
    )
    
    oi_change_pct: Optional[Decimal] = Field(
        default=None,
        description="OI change percentage"
    )
    
    # ========================
    # Calculated Metrics
    # ========================
    vwap: Optional[Decimal] = Field(
        default=None,
        description="Volume Weighted Average Price"
    )
    
    atp: Optional[Decimal] = Field(
        default=None,
        description="Average Traded Price (from Upstox)"
    )
    
    price_vwap_deviation: Optional[Decimal] = Field(
        default=None,
        description="Price deviation from VWAP"
    )
    
    # ========================
    # Support Levels (Top 3)
    # ========================
    support_level_1: Optional[Decimal] = Field(default=None)
    support_qty_1: Optional[int] = Field(default=None)
    
    support_level_2: Optional[Decimal] = Field(default=None)
    support_qty_2: Optional[int] = Field(default=None)
    
    support_level_3: Optional[Decimal] = Field(default=None)
    support_qty_3: Optional[int] = Field(default=None)
    
    support: Optional[Decimal] = Field(
        default=None,
        description="Average support level"
    )
    
    # ========================
    # Resistance Levels (Top 3)
    # ========================
    resistance_level_1: Optional[Decimal] = Field(default=None)
    resistance_qty_1: Optional[int] = Field(default=None)
    
    resistance_level_2: Optional[Decimal] = Field(default=None)
    resistance_qty_2: Optional[int] = Field(default=None)
    
    resistance_level_3: Optional[Decimal] = Field(default=None)
    resistance_qty_3: Optional[int] = Field(default=None)
    
    resistance: Optional[Decimal] = Field(
        default=None,
        description="Average resistance level"
    )
    
    # ========================
    # Order Book Metrics
    # ========================
    tbq: Optional[int] = Field(
        default=None,
        description="Total Bid Quantity"
    )
    
    tsq: Optional[int] = Field(
        default=None,
        description="Total Sell Quantity"
    )
    
    order_book_ratio: Optional[Decimal] = Field(
        default=None,
        description="TBQ/(TBQ+TSQ) - buyer/seller pressure"
    )
    
    bid_ask_spread: Optional[Decimal] = Field(
        default=None,
        description="Bid-Ask spread"
    )
    
    big_bid_count: Optional[int] = Field(
        default=None,
        description="Count of whale bids"
    )
    
    big_ask_count: Optional[int] = Field(
        default=None,
        description="Count of whale asks"
    )
    
    # ========================
    # Greeks (Averaged)
    # ========================
    avg_delta: Optional[Decimal] = Field(default=None)
    avg_gamma: Optional[Decimal] = Field(default=None)
    avg_theta: Optional[Decimal] = Field(default=None)
    avg_vega: Optional[Decimal] = Field(default=None)
    avg_rho: Optional[Decimal] = Field(default=None)
    avg_iv: Optional[Decimal] = Field(default=None)
    
    gamma_spike: Optional[Decimal] = Field(
        default=None,
        description="Gamma spike percentage"
    )
    
    # ========================
    # Candle Score
    # ========================
    candle_score: Optional[Decimal] = Field(
        default=None,
        description="Candle importance score"
    )
    
    # ========================
    # Metadata
    # ========================
    tick_count: Optional[int] = Field(
        default=None,
        description="Number of ticks in this candle"
    )


# ========================
# Testing
# ========================
if __name__ == "__main__":
    """
    Test candle event
    Run: uv run python src/events/candle_events.py
    """
    from datetime import datetime
    import sys
    from pathlib import Path
    
    # Add project to path
    project_root = Path(__file__).parent.parent.parent
    sys.path.insert(0, str(project_root))
    from src.utils.timezone import IST
    
    print("=" * 70)
    print("Candle Event Test")
    print("=" * 70)
    print()
    
    # Create sample candle
    candle = CandleCompletedEvent(
        instrument_key="NSE_FO|61755",
        candle_timestamp=datetime(2024, 11, 16, 9, 15, 0, tzinfo=IST),
        
        # OHLC
        open=Decimal("182.00"),
        high=Decimal("183.50"),
        low=Decimal("181.50"),
        close=Decimal("182.50"),
        previous_close=Decimal("180.00"),
        
        # Volume
        volume=125000,
        oi=8326800,
        oi_change=Decimal("50000"),
        oi_change_pct=Decimal("0.0060"),
        
        # Metrics
        vwap=Decimal("182.25"),
        price_vwap_deviation=Decimal("0.0014"),
        
        # Support
        support_level_1=Decimal("182.00"),
        support_qty_1=1950,
        support_level_2=Decimal("181.95"),
        support_qty_2=900,
        support_level_3=Decimal("181.90"),
        support_qty_3=1350,
        support=Decimal("181.95"),
        
        # Resistance
        resistance_level_1=Decimal("182.45"),
        resistance_qty_1=675,
        resistance_level_2=Decimal("182.50"),
        resistance_qty_2=1800,
        resistance_level_3=Decimal("182.55"),
        resistance_qty_3=1200,
        resistance=Decimal("182.50"),
        
        # Order book
        tbq=4185525,
        tsq=901350,
        order_book_ratio=Decimal("0.8227"),
        bid_ask_spread=Decimal("0.0019"),
        big_bid_count=3,
        big_ask_count=1,
        
        # Greeks
        avg_delta=Decimal("0.4519"),
        avg_gamma=Decimal("0.0007"),
        avg_theta=Decimal("-17.6157"),
        avg_vega=Decimal("12.7741"),
        avg_rho=Decimal("1.8554"),
        avg_iv=Decimal("0.1685"),
        gamma_spike=Decimal("0.0250"),
        
        # Score
        candle_score=Decimal("125000.50"),
        tick_count=85
    )
    
    print("1. Candle Event Created:")
    print("-" * 70)
    print(f"   Instrument:    {candle.instrument_key}")
    print(f"   Timestamp:     {candle.candle_timestamp}")
    print(f"   Tick Count:    {candle.tick_count}")
    print()
    
    print("2. OHLC:")
    print("-" * 70)
    print(f"   Open:          {candle.open}")
    print(f"   High:          {candle.high}")
    print(f"   Low:           {candle.low}")
    print(f"   Close:         {candle.close}")
    print(f"   VWAP:          {candle.vwap}")
    print()
    
    print("3. Volume & OI:")
    print("-" * 70)
    print(f"   Volume:        {candle.volume:,}")
    print(f"   OI:            {candle.oi:,}")
    print(f"   OI Change:     {candle.oi_change:,} ({candle.oi_change_pct}%)")
    print()
    
    print("4. Support & Resistance:")
    print("-" * 70)
    print(f"   Support Lvl 1: {candle.support_level_1} x {candle.support_qty_1}")
    print(f"   Support Avg:   {candle.support}")
    print(f"   Resistance 1:  {candle.resistance_level_1} x {candle.resistance_qty_1}")
    print(f"   Resistance Avg:{candle.resistance}")
    print()
    
    print("5. Order Book:")
    print("-" * 70)
    print(f"   TBQ:           {candle.tbq:,}")
    print(f"   TSQ:           {candle.tsq:,}")
    print(f"   Ratio:         {candle.order_book_ratio}")
    print(f"   Spread:        {candle.bid_ask_spread}")
    print()
    
    print("6. Greeks:")
    print("-" * 70)
    print(f"   Delta:         {candle.avg_delta}")
    print(f"   Gamma:         {candle.avg_gamma}")
    print(f"   Gamma Spike:   {candle.gamma_spike}")
    print(f"   IV:            {candle.avg_iv}")
    print()
    
    print("7. Candle Score:")
    print("-" * 70)
    print(f"   Score:         {candle.candle_score}")
    print()
    
    print("8. JSON Serialization:")
    print("-" * 70)
    json_str = candle.to_json()
    print(f"   JSON length:   {len(json_str)} bytes")
    
    # Deserialize
    reconstructed = CandleCompletedEvent.from_json(json_str)
    print(f"   Close match:   {reconstructed.close == candle.close}")
    print(f"   Score match:   {reconstructed.candle_score == candle.candle_score}")
    print()
    
    print("=" * 70)
    print("✅ Candle event working!")
    print("=" * 70)