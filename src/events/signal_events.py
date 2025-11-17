"""
Signal Events
Events for seller state detection and buy/sell signals
"""

from datetime import datetime
from decimal import Decimal
from typing import Optional, List
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


class SellerStateDetectedEvent(BaseEvent):
    """
    Event emitted when seller behavior state is detected
    
    States:
    - SELLER_PANIC: Sellers desperately exiting (BUY signal)
    - PROFIT_BOOKING: Sellers taking profits (WATCH signal)
    - SELLER_DIRECTION: Sellers confident (WAIT signal)
    - NEUTRAL: No clear state
    """
    
    event_type: str = "seller_state.detected"
    
    # ========================
    # Identification
    # ========================
    instrument_key: str = Field(
        description="Instrument identifier"
    )
    
    detection_timestamp: datetime = Field(
        description="When state was detected (IST)"
    )
    
    # ========================
    # Detected State
    # ========================
    state: str = Field(
        description="Detected seller state (PANIC, PROFIT_BOOKING, DIRECTION, NEUTRAL)"
    )
    
    confidence: Decimal = Field(
        description="Confidence level (0.0 to 1.0)"
    )
    
    panic_score: Decimal = Field(
        description="Panic score (0 to 100)"
    )
    
    # ========================
    # Detected Signals
    # ========================
    signals: List[str] = Field(
        default_factory=list,
        description="List of detected signal triggers"
    )
    
    # Signal details
    short_covering: bool = Field(
        default=False,
        description="OI decrease + price increase detected"
    )
    
    gamma_spike_detected: bool = Field(
        default=False,
        description="Gamma spike > threshold"
    )
    
    order_book_panic: bool = Field(
        default=False,
        description="Ask-heavy order book (sellers dumping)"
    )
    
    liquidity_drying: bool = Field(
        default=False,
        description="Spread widening (low liquidity)"
    )
    
    strong_buying: bool = Field(
        default=False,
        description="Price significantly above VWAP"
    )
    
    # ========================
    # Recommendation
    # ========================
    recommendation: str = Field(
        description="Trading recommendation (BUY, SELL, WAIT)"
    )
    
    entry_price: Optional[Decimal] = Field(
        default=None,
        description="Suggested entry price for BUY signal"
    )
    
    suggested_quantity: Optional[int] = Field(
        default=None,
        description="Suggested quantity"
    )


class BuySignalGeneratedEvent(BaseEvent):
    """
    Event emitted when a BUY signal is generated
    Strong conviction to buy the option
    """
    
    event_type: str = "signal.buy"
    
    # ========================
    # Identification
    # ========================
    instrument_key: str = Field(
        description="Instrument identifier"
    )
    
    signal_timestamp: datetime = Field(
        description="When signal was generated (IST)"
    )
    
    # ========================
    # Signal Strength
    # ========================
    signal_strength: Decimal = Field(
        description="Signal strength (0-100)"
    )
    
    # ========================
    # Trigger Reasons
    # ========================
    panic_detected: bool = Field(
        default=False,
        description="Seller panic detected"
    )
    
    gamma_spike: Optional[Decimal] = Field(
        default=None,
        description="Gamma spike value"
    )
    
    iv_rise: Optional[Decimal] = Field(
        default=None,
        description="IV rise value"
    )
    
    vwap_breach: bool = Field(
        default=False,
        description="Price breached VWAP significantly"
    )
    
    # ========================
    # Trading Recommendation
    # ========================
    entry_price: Decimal = Field(
        description="Recommended entry price"
    )
    
    stop_loss: Optional[Decimal] = Field(
        default=None,
        description="Suggested stop loss"
    )
    
    target: Optional[Decimal] = Field(
        default=None,
        description="Suggested target price"
    )
    
    suggested_quantity: int = Field(
        description="Suggested quantity to buy"
    )
    
    risk_reward_ratio: Optional[Decimal] = Field(
        default=None,
        description="Risk/Reward ratio"
    )


class SellSignalGeneratedEvent(BaseEvent):
    """
    Event emitted when a SELL signal is generated
    Exit or short the option
    """
    
    event_type: str = "signal.sell"
    
    # ========================
    # Identification
    # ========================
    instrument_key: str = Field(
        description="Instrument identifier"
    )
    
    signal_timestamp: datetime = Field(
        description="When signal was generated (IST)"
    )
    
    # ========================
    # Signal Details
    # ========================
    signal_strength: Decimal = Field(
        description="Signal strength (0-100)"
    )
    
    reason: str = Field(
        description="Reason for SELL signal"
    )
    
    exit_price: Decimal = Field(
        description="Recommended exit price"
    )


# ========================
# Testing
# ========================
if __name__ == "__main__":
    """
    Test signal events
    Run: uv run python src/events/signal_events.py
    """
    from datetime import datetime
    import sys
    from pathlib import Path
    
    # Add project to path
    project_root = Path(__file__).parent.parent.parent
    sys.path.insert(0, str(project_root))
    from src.utils.timezone import IST
    
    print("=" * 70)
    print("Signal Events Test")
    print("=" * 70)
    print()
    
    # Test 1: Seller State Detected
    print("1. Seller State Detected Event:")
    print("-" * 70)
    
    seller_state = SellerStateDetectedEvent(
        instrument_key="NSE_FO|61755",
        detection_timestamp=datetime(2024, 11, 16, 9, 16, 0, tzinfo=IST),
        state="SELLER_PANIC",
        confidence=Decimal("0.85"),
        panic_score=Decimal("75.5"),
        signals=["SHORT_COVERING", "GAMMA_SPIKE", "ASK_HEAVY_DUMPING"],
        short_covering=True,
        gamma_spike_detected=True,
        order_book_panic=True,
        liquidity_drying=False,
        strong_buying=True,
        recommendation="BUY",
        entry_price=Decimal("182.50"),
        suggested_quantity=50
    )
    
    print(f"   Instrument:    {seller_state.instrument_key}")
    print(f"   State:         {seller_state.state}")
    print(f"   Confidence:    {seller_state.confidence}")
    print(f"   Panic Score:   {seller_state.panic_score}")
    print(f"   Signals:       {', '.join(seller_state.signals)}")
    print(f"   Recommendation:{seller_state.recommendation}")
    print(f"   Entry Price:   {seller_state.entry_price}")
    print()
    
    # Test 2: Buy Signal
    print("2. Buy Signal Generated Event:")
    print("-" * 70)
    
    buy_signal = BuySignalGeneratedEvent(
        instrument_key="NSE_FO|61755",
        signal_timestamp=datetime(2024, 11, 16, 9, 16, 30, tzinfo=IST),
        signal_strength=Decimal("85.0"),
        panic_detected=True,
        gamma_spike=Decimal("0.55"),
        iv_rise=Decimal("0.02"),
        vwap_breach=True,
        entry_price=Decimal("182.50"),
        stop_loss=Decimal("178.00"),
        target=Decimal("190.00"),
        suggested_quantity=50,
        risk_reward_ratio=Decimal("1.67")
    )
    
    print(f"   Instrument:    {buy_signal.instrument_key}")
    print(f"   Strength:      {buy_signal.signal_strength}")
    print(f"   Entry:         {buy_signal.entry_price}")
    print(f"   Stop Loss:     {buy_signal.stop_loss}")
    print(f"   Target:        {buy_signal.target}")
    print(f"   Quantity:      {buy_signal.suggested_quantity}")
    print(f"   Risk/Reward:   {buy_signal.risk_reward_ratio}")
    print()
    
    # Test 3: Sell Signal
    print("3. Sell Signal Generated Event:")
    print("-" * 70)
    
    sell_signal = SellSignalGeneratedEvent(
        instrument_key="NSE_FO|61755",
        signal_timestamp=datetime(2024, 11, 16, 14, 30, 0, tzinfo=IST),
        signal_strength=Decimal("70.0"),
        reason="Target reached",
        exit_price=Decimal("190.00")
    )
    
    print(f"   Instrument:    {sell_signal.instrument_key}")
    print(f"   Strength:      {sell_signal.signal_strength}")
    print(f"   Reason:        {sell_signal.reason}")
    print(f"   Exit Price:    {sell_signal.exit_price}")
    print()
    
    # Test JSON serialization
    print("4. JSON Serialization:")
    print("-" * 70)
    json_str = buy_signal.to_json()
    print(f"   JSON length:   {len(json_str)} bytes")
    
    reconstructed = BuySignalGeneratedEvent.from_json(json_str)
    print(f"   Entry match:   {reconstructed.entry_price == buy_signal.entry_price}")
    print(f"   Strength match:{reconstructed.signal_strength == buy_signal.signal_strength}")
    print()
    
    print("=" * 70)
    print("✅ All signal events working!")
    print("=" * 70)