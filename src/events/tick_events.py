"""
Tick Events
Events for real-time market tick data from Upstox
"""

from datetime import datetime
from decimal import Decimal
from typing import Optional, List
from pydantic import Field

# Handle imports
try:
    from .base import BaseEvent
    from ..utils.timezone import parse_tick_timestamp, candle_minute
except ImportError:
    import sys
    from pathlib import Path
    project_root = Path(__file__).parent.parent.parent
    sys.path.insert(0, str(project_root))
    from src.events.base import BaseEvent
    from src.utils.timezone import parse_tick_timestamp, candle_minute


class TickReceivedEvent(BaseEvent):
    """
    Event emitted when a tick is received from Upstox
    
    Contains all market data from Upstox feed:
    - LTP, volume, OI
    - 30-level order book
    - Option Greeks
    - TBQ/TSQ
    """
    
    event_type: str = "tick.received"
    
    # ========================
    # Metadata
    # ========================
    instrument_key: str = Field(
        description="Upstox instrument key (e.g., 'NSE_FO|61755')"
    )
    
    raw_timestamp: str = Field(
        description="Original Upstox timestamp (milliseconds or ISO format)"
    )
    
    candle_time: datetime = Field(
        description="IST candle boundary (e.g., 09:15:00, 09:16:00)"
    )
    
    # ========================
    # Price & Volume
    # ========================
    ltp: Decimal = Field(
        description="Last Traded Price"
    )
    
    ltq: int = Field(
        default=0,
        description="Last Traded Quantity"
    )
    
    volume: int = Field(
        description="Total volume traded"
    )
    
    oi: int = Field(
        description="Open Interest"
    )
    
    # ========================
    # Additional Price Data
    # ========================
    atp: Optional[Decimal] = Field(
        default=None,
        description="Average Traded Price (from Upstox)"
    )
    
    previous_close: Optional[Decimal] = Field(
        default=None,
        description="Previous day's close price"
    )
    
    # ========================
    # Order Book (30 levels)
    # ========================
    bid_prices: List[Decimal] = Field(
        default_factory=list,
        description="Bid prices (30 levels)"
    )
    
    bid_quantities: List[int] = Field(
        default_factory=list,
        description="Bid quantities (30 levels)"
    )
    
    ask_prices: List[Decimal] = Field(
        default_factory=list,
        description="Ask prices (30 levels)"
    )
    
    ask_quantities: List[int] = Field(
        default_factory=list,
        description="Ask quantities (30 levels)"
    )
    
    # ========================
    # Order Book Summary
    # ========================
    tbq: Optional[int] = Field(
        default=None,
        description="Total Bid Quantity (from Upstox)"
    )
    
    tsq: Optional[int] = Field(
        default=None,
        description="Total Sell Quantity (from Upstox)"
    )
    
    # ========================
    # Greeks
    # ========================
    delta: Optional[float] = Field(
        default=None,
        description="Option delta"
    )
    
    gamma: Optional[float] = Field(
        default=None,
        description="Option gamma"
    )
    
    theta: Optional[float] = Field(
        default=None,
        description="Option theta"
    )
    
    vega: Optional[float] = Field(
        default=None,
        description="Option vega"
    )
    
    rho: Optional[float] = Field(
        default=None,
        description="Option rho"
    )
    
    iv: Optional[float] = Field(
        default=None,
        description="Implied Volatility"
    )
    
    @classmethod
    def from_upstox_feed(cls, instrument_key: str, feed_data: dict) -> "TickReceivedEvent":
        """
        Create TickReceivedEvent from Upstox market feed
        
        Args:
            instrument_key: Instrument identifier
            feed_data: Market feed data from Upstox
            
        Returns:
            TickReceivedEvent instance
            
        Example Upstox feed structure:
        {
            "fullFeed": {
                "marketFF": {
                    "ltpc": {"ltp": 181.95, "ltt": "1747984841612", "ltq": "75", "cp": 73.85},
                    "marketLevel": {"bidAskQuote": [...]},
                    "optionGreeks": {"delta": 0.4519, ...},
                    "oi": 8326800,
                    "iv": 0.1685,
                    "tbq": 4185525,
                    "tsq": 901350,
                    "atp": 139.42,
                    "vtt": "119687250"
                }
            }
        }
        """
        market_data = feed_data.get("fullFeed", {}).get("marketFF", {})
        
        # Parse price data
        ltpc = market_data.get("ltpc", {})
        ltp = Decimal(str(ltpc.get("ltp", 0)))
        raw_ts = ltpc.get("ltt", "")
        ltq = int(ltpc.get("ltq", 0))
        cp = ltpc.get("cp")
        
        # Parse timestamp and get candle boundary
        ist_time = parse_tick_timestamp(raw_ts) if raw_ts else None
        candle_boundary = candle_minute(ist_time) if ist_time else None
        
        # Parse order book
        bid_ask_quotes = market_data.get("marketLevel", {}).get("bidAskQuote", [])
        
        bid_prices = []
        bid_quantities = []
        ask_prices = []
        ask_quantities = []
        
        for quote in bid_ask_quotes:
            if "bidP" in quote:
                bid_prices.append(Decimal(str(quote["bidP"])))
            if "bidQ" in quote:
                bid_quantities.append(int(quote["bidQ"]))
            if "askP" in quote:
                ask_prices.append(Decimal(str(quote["askP"])))
            if "askQ" in quote:
                ask_quantities.append(int(quote["askQ"]))
        
        # Parse Greeks
        greeks = market_data.get("optionGreeks", {})
        
        # Volume data
        volume = int(market_data.get("vtt", 0))
        
        return cls(
            instrument_key=instrument_key,
            raw_timestamp=raw_ts,
            timestamp=ist_time,
            candle_time=candle_boundary,
            
            # Price
            ltp=ltp,
            ltq=ltq,
            volume=volume,
            oi=market_data.get("oi", 0),
            atp=Decimal(str(market_data["atp"])) if "atp" in market_data else None,
            previous_close=Decimal(str(cp)) if cp else None,
            
            # Order book
            bid_prices=bid_prices,
            bid_quantities=bid_quantities,
            ask_prices=ask_prices,
            ask_quantities=ask_quantities,
            
            # TBQ/TSQ
            tbq=market_data.get("tbq"),
            tsq=market_data.get("tsq"),
            
            # Greeks
            delta=greeks.get("delta"),
            gamma=greeks.get("gamma"),
            theta=greeks.get("theta"),
            vega=greeks.get("vega"),
            rho=greeks.get("rho"),
            iv=market_data.get("iv"),
        )


# ========================
# Testing
# ========================
if __name__ == "__main__":
    """
    Test tick event with sample Upstox data
    Run: uv run python src/events/tick_events.py
    """
    import json
    
    print("=" * 70)
    print("Tick Event Test")
    print("=" * 70)
    print()
    
    # Sample Upstox feed (from your example)
    sample_feed = {
        "fullFeed": {
            "marketFF": {
                "ltpc": {
                    "ltp": 181.95,
                    "ltt": "1747984841612",
                    "ltq": "75",
                    "cp": 73.85
                },
                "marketLevel": {
                    "bidAskQuote": [
                        {"bidQ": "600", "bidP": 182.05, "askQ": "750", "askP": 182.4},
                        {"bidQ": "1950", "bidP": 182, "askQ": "675", "askP": 182.45},
                        {"bidQ": "900", "bidP": 181.95, "askQ": "1800", "askP": 182.5},
                    ]
                },
                "optionGreeks": {
                    "delta": 0.4519,
                    "theta": -17.6157,
                    "gamma": 0.0007,
                    "vega": 12.7741,
                    "rho": 1.8554
                },
                "oi": 8326800,
                "iv": 0.1685333251953125,
                "tbq": 4185525,
                "tsq": 901350,
                "atp": 139.42,
                "vtt": "119687250"
            }
        }
    }
    
    # Create event from Upstox feed
    tick_event = TickReceivedEvent.from_upstox_feed(
        instrument_key="NSE_FO|61755",
        feed_data=sample_feed
    )
    
    print("1. Tick Event Created from Upstox Feed:")
    print("-" * 70)
    print(f"   Instrument:    {tick_event.instrument_key}")
    print(f"   LTP:           {tick_event.ltp}")
    print(f"   Volume:        {tick_event.volume:,}")
    print(f"   OI:            {tick_event.oi:,}")
    print(f"   Timestamp:     {tick_event.timestamp}")
    print(f"   Candle Time:   {tick_event.candle_time}")
    print()
    
    print("2. Order Book:")
    print("-" * 70)
    print(f"   Bid levels:    {len(tick_event.bid_prices)}")
    print(f"   Ask levels:    {len(tick_event.ask_prices)}")
    print(f"   Best Bid:      {tick_event.bid_prices[0]} x {tick_event.bid_quantities[0]}")
    print(f"   Best Ask:      {tick_event.ask_prices[0]} x {tick_event.ask_quantities[0]}")
    print(f"   TBQ:           {tick_event.tbq:,}")
    print(f"   TSQ:           {tick_event.tsq:,}")
    print()
    
    print("3. Greeks:")
    print("-" * 70)
    print(f"   Delta:         {tick_event.delta}")
    print(f"   Gamma:         {tick_event.gamma}")
    print(f"   Theta:         {tick_event.theta}")
    print(f"   Vega:          {tick_event.vega}")
    print(f"   Rho:           {tick_event.rho}")
    print(f"   IV:            {tick_event.iv}")
    print()
    
    print("4. JSON Serialization:")
    print("-" * 70)
    json_str = tick_event.to_json()
    print(f"   JSON length:   {len(json_str)} bytes")
    
    # Deserialize
    reconstructed = TickReceivedEvent.from_json(json_str)
    print(f"   LTP match:     {reconstructed.ltp == tick_event.ltp}")
    print(f"   OI match:      {reconstructed.oi == tick_event.oi}")
    print()
    
    print("=" * 70)
    print("✅ Tick event working with Upstox data structure!")
    print("=" * 70)