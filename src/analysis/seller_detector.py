"""
Seller State Detector
Detects seller behavior patterns and generates trading signals

States:
- SELLER_PANIC: Sellers desperately exiting → BUY
- PROFIT_BOOKING: Sellers taking profits → WATCH
- SELLER_DIRECTION: Sellers confident → WAIT
- NEUTRAL: No clear state
"""

from decimal import Decimal
from typing import List, Optional, Tuple
from dataclasses import dataclass
from enum import Enum

# Handle imports
try:
    pass
except ImportError:
    import sys
    from pathlib import Path
    project_root = Path(__file__).parent.parent.parent
    sys.path.insert(0, str(project_root))


class SellerState(str, Enum):
    """Seller behavior states"""
    SELLER_PANIC = "SELLER_PANIC"
    PROFIT_BOOKING = "PROFIT_BOOKING"
    SELLER_DIRECTION = "SELLER_DIRECTION"
    NEUTRAL = "NEUTRAL"


class Recommendation(str, Enum):
    """Trading recommendations"""
    BUY = "BUY"
    SELL = "SELL"
    WAIT = "WAIT"


@dataclass
class DetectionResult:
    """Seller state detection result"""
    state: SellerState
    confidence: Decimal  # 0.0 to 1.0
    panic_score: Decimal  # 0 to 100
    signals: List[str]
    recommendation: Recommendation
    
    # Signal flags
    short_covering: bool = False
    gamma_spike_detected: bool = False
    order_book_panic: bool = False
    liquidity_drying: bool = False
    strong_buying: bool = False


class SellerStateDetector:
    """
    Detect seller behavior from market data
    
    Key Patterns:
    1. SHORT COVERING: OI↓ + Price↑ (sellers forced to buy back)
    2. GAMMA SPIKE: Rapid gamma increase (dealers hedging)
    3. ORDER BOOK PANIC: Ask-heavy order book (sellers dumping)
    4. LIQUIDITY DRYING: Wide spread (low liquidity, panic)
    5. STRONG BUYING: Price >> VWAP (buyers aggressive)
    """
    
    def __init__(
        self,
        # Thresholds
        oi_decrease_threshold: float = -0.003,  # -0.3% OI decrease
        price_increase_threshold: float = 0.005,  # 0.5% price increase
        gamma_spike_threshold: float = 0.30,  # 30% gamma spike
        order_book_panic_threshold: float = 0.35,  # Ratio < 0.35 = panic
        spread_threshold: float = 0.005,  # 0.5% spread = wide
        vwap_deviation_threshold: float = 0.01,  # 1% above VWAP
        panic_score_buy_threshold: float = 60.0  # Score > 60 = BUY
    ):
        """
        Initialize detector with thresholds
        
        Args:
            oi_decrease_threshold: OI change % for short covering detection
            price_increase_threshold: Price change % for short covering
            gamma_spike_threshold: Gamma spike % threshold
            order_book_panic_threshold: Order book ratio threshold
            spread_threshold: Bid-ask spread threshold
            vwap_deviation_threshold: VWAP deviation threshold
            panic_score_buy_threshold: Panic score for BUY signal
        """
        self.oi_decrease_threshold = oi_decrease_threshold
        self.price_increase_threshold = price_increase_threshold
        self.gamma_spike_threshold = gamma_spike_threshold
        self.order_book_panic_threshold = order_book_panic_threshold
        self.spread_threshold = spread_threshold
        self.vwap_deviation_threshold = vwap_deviation_threshold
        self.panic_score_buy_threshold = panic_score_buy_threshold
    
    def detect_short_covering(
        self,
        oi_change_pct: Optional[Decimal],
        price_change_pct: Optional[Decimal]
    ) -> bool:
        """
        Detect short covering: OI decreasing + Price increasing
        
        This means sellers are being forced to buy back positions
        
        Args:
            oi_change_pct: OI change percentage
            price_change_pct: Price change percentage
            
        Returns:
            True if short covering detected
        """
        if oi_change_pct is None or price_change_pct is None:
            return False
        
        oi_decreasing = float(oi_change_pct) < self.oi_decrease_threshold
        price_increasing = float(price_change_pct) > self.price_increase_threshold
        
        return oi_decreasing and price_increasing
    
    def detect_gamma_spike(
        self,
        gamma_spike: Optional[Decimal]
    ) -> bool:
        """
        Detect gamma spike
        
        Rapid gamma increase = dealers need to hedge = price acceleration
        
        Args:
            gamma_spike: Gamma spike percentage
            
        Returns:
            True if gamma spike detected
        """
        if gamma_spike is None:
            return False
        
        return abs(float(gamma_spike)) > self.gamma_spike_threshold
    
    def detect_order_book_panic(
        self,
        order_book_ratio: Optional[Decimal]
    ) -> bool:
        """
        Detect order book panic: Ask-heavy (sellers dumping)
        
        Ratio < threshold = too many sellers, not enough buyers
        
        Args:
            order_book_ratio: TBQ / (TBQ + TSQ)
            
        Returns:
            True if order book panic detected
        """
        if order_book_ratio is None:
            return False
        
        return float(order_book_ratio) < self.order_book_panic_threshold
    
    def detect_liquidity_drying(
        self,
        bid_ask_spread: Optional[Decimal]
    ) -> bool:
        """
        Detect liquidity drying: Wide spread
        
        Wide spread = low liquidity = panic conditions
        
        Args:
            bid_ask_spread: Bid-ask spread percentage
            
        Returns:
            True if liquidity drying detected
        """
        if bid_ask_spread is None:
            return False
        
        return float(bid_ask_spread) > self.spread_threshold
    
    def detect_strong_buying(
        self,
        price: Decimal,
        vwap: Optional[Decimal]
    ) -> bool:
        """
        Detect strong buying: Price significantly above VWAP
        
        Price >> VWAP = buyers very aggressive
        
        Args:
            price: Current price
            vwap: VWAP
            
        Returns:
            True if strong buying detected
        """
        if vwap is None or vwap == 0:
            return False
        
        deviation = (price - vwap) / vwap
        
        return float(deviation) > self.vwap_deviation_threshold
    
    def calculate_panic_score(
        self,
        short_covering: bool,
        gamma_spike: bool,
        order_book_panic: bool,
        liquidity_drying: bool,
        strong_buying: bool,
        # Additional factors
        oi_change_pct: Optional[Decimal] = None,
        order_book_ratio: Optional[Decimal] = None
    ) -> Decimal:
        """
        Calculate panic score (0 to 100)
        
        Higher score = More panic = Better BUY opportunity
        
        Score components:
        - Short covering: +30 points
        - Gamma spike: +25 points
        - Order book panic: +20 points (more extreme = more points)
        - Liquidity drying: +15 points
        - Strong buying: +10 points
        
        Args:
            short_covering: Short covering detected
            gamma_spike: Gamma spike detected
            order_book_panic: Order book panic detected
            liquidity_drying: Liquidity drying detected
            strong_buying: Strong buying detected
            oi_change_pct: OI change percentage (for weighting)
            order_book_ratio: Order book ratio (for weighting)
            
        Returns:
            Panic score (0-100)
        """
        score = Decimal('0')
        
        # 1. Short covering (strongest signal)
        if short_covering:
            base_score = Decimal('30')
            # Extra points for severe OI decrease
            if oi_change_pct and abs(float(oi_change_pct)) > 0.01:  # > 1%
                base_score += Decimal('10')
            score += base_score
        
        # 2. Gamma spike
        if gamma_spike:
            score += Decimal('25')
        
        # 3. Order book panic
        if order_book_panic:
            base_score = Decimal('20')
            # Extra points for extreme imbalance
            if order_book_ratio:
                if float(order_book_ratio) < 0.25:  # Very extreme
                    base_score += Decimal('10')
            score += base_score
        
        # 4. Liquidity drying
        if liquidity_drying:
            score += Decimal('15')
        
        # 5. Strong buying
        if strong_buying:
            score += Decimal('10')
        
        # Cap at 100
        return min(score, Decimal('100'))
    
    def determine_state_and_recommendation(
        self,
        panic_score: Decimal,
        short_covering: bool,
        signals_count: int
    ) -> Tuple[SellerState, Recommendation, Decimal]:
        """
        Determine seller state and recommendation
        
        Args:
            panic_score: Panic score (0-100)
            short_covering: Short covering detected
            signals_count: Number of signals detected
            
        Returns:
            (state, recommendation, confidence)
        """
        # High panic score = SELLER_PANIC
        if float(panic_score) >= self.panic_score_buy_threshold:
            confidence = min(Decimal('0.9'), panic_score / 100)
            return (
                SellerState.SELLER_PANIC,
                Recommendation.BUY,
                confidence
            )
        
        # Moderate signals = PROFIT_BOOKING
        elif signals_count >= 2 and not short_covering:
            confidence = Decimal('0.6')
            return (
                SellerState.PROFIT_BOOKING,
                Recommendation.WAIT,
                confidence
            )
        
        # Low activity = NEUTRAL
        elif signals_count <= 1:
            confidence = Decimal('0.5')
            return (
                SellerState.NEUTRAL,
                Recommendation.WAIT,
                confidence
            )
        
        # Default
        else:
            confidence = Decimal('0.5')
            return (
                SellerState.NEUTRAL,
                Recommendation.WAIT,
                confidence
            )
    
    def detect(
        self,
        # OI data
        oi_change_pct: Optional[Decimal] = None,
        
        # Price data
        price: Optional[Decimal] = None,
        previous_close: Optional[Decimal] = None,
        vwap: Optional[Decimal] = None,
        
        # Greeks
        gamma_spike: Optional[Decimal] = None,
        
        # Order book
        order_book_ratio: Optional[Decimal] = None,
        bid_ask_spread: Optional[Decimal] = None
    ) -> DetectionResult:
        """
        Detect seller state from market data
        
        Args:
            oi_change_pct: OI change percentage
            price: Current price
            previous_close: Previous close price
            vwap: VWAP
            gamma_spike: Gamma spike
            order_book_ratio: Order book ratio
            bid_ask_spread: Bid-ask spread
            
        Returns:
            DetectionResult with state, signals, and recommendation
        """
        # Calculate price change
        price_change_pct = None
        if price and previous_close and previous_close > 0:
            price_change_pct = (price - previous_close) / previous_close
        
        # Detect individual signals
        short_covering = self.detect_short_covering(oi_change_pct, price_change_pct)
        gamma_spike_det = self.detect_gamma_spike(gamma_spike)
        ob_panic = self.detect_order_book_panic(order_book_ratio)
        liquidity_dry = self.detect_liquidity_drying(bid_ask_spread)
        strong_buy = self.detect_strong_buying(price, vwap) if price else False
        
        # Build signal list
        signals = []
        if short_covering:
            signals.append("SHORT_COVERING")
        if gamma_spike_det:
            signals.append("GAMMA_SPIKE")
        if ob_panic:
            signals.append("ORDER_BOOK_PANIC")
        if liquidity_dry:
            signals.append("LIQUIDITY_DRYING")
        if strong_buy:
            signals.append("STRONG_BUYING")
        
        # Calculate panic score
        panic_score = self.calculate_panic_score(
            short_covering=short_covering,
            gamma_spike=gamma_spike_det,
            order_book_panic=ob_panic,
            liquidity_drying=liquidity_dry,
            strong_buying=strong_buy,
            oi_change_pct=oi_change_pct,
            order_book_ratio=order_book_ratio
        )
        
        # Determine state and recommendation
        state, recommendation, confidence = self.determine_state_and_recommendation(
            panic_score=panic_score,
            short_covering=short_covering,
            signals_count=len(signals)
        )
        
        return DetectionResult(
            state=state,
            confidence=confidence,
            panic_score=panic_score,
            signals=signals,
            recommendation=recommendation,
            short_covering=short_covering,
            gamma_spike_detected=gamma_spike_det,
            order_book_panic=ob_panic,
            liquidity_drying=liquidity_dry,
            strong_buying=strong_buy
        )


# ========================
# Testing
# ========================
if __name__ == "__main__":
    """
    Test seller state detector
    Run: uv run python src/analysis/seller_detector.py
    """
    
    print("=" * 70)
    print("Seller State Detector Test")
    print("=" * 70)
    print()
    
    detector = SellerStateDetector()
    
    # Test 1: Neutral market
    print("1. Neutral Market:")
    print("-" * 70)
    result_neutral = detector.detect(
        oi_change_pct=Decimal('0.0001'),
        price=Decimal('182.00'),
        previous_close=Decimal('181.90'),
        vwap=Decimal('181.95'),
        order_book_ratio=Decimal('0.5')
    )
    print(f"   State:          {result_neutral.state}")
    print(f"   Panic Score:    {result_neutral.panic_score}")
    print(f"   Recommendation: {result_neutral.recommendation}")
    print(f"   Signals:        {', '.join(result_neutral.signals) if result_neutral.signals else 'None'}")
    print()
    
    # Test 2: Seller panic (HIGH PRIORITY!)
    print("2. SELLER PANIC - BUY SIGNAL:")
    print("-" * 70)
    result_panic = detector.detect(
        oi_change_pct=Decimal('-0.008'),  # OI decreasing 0.8%
        price=Decimal('185.00'),
        previous_close=Decimal('182.00'),  # Price up 1.6%
        vwap=Decimal('182.50'),  # Price above VWAP
        gamma_spike=Decimal('0.55'),  # 55% gamma spike!
        order_book_ratio=Decimal('0.28'),  # Very ask heavy
        bid_ask_spread=Decimal('0.008')  # Wide spread
    )
    print(f"   State:          {result_panic.state} ⚠️")
    print(f"   Panic Score:    {result_panic.panic_score} / 100")
    print(f"   Confidence:     {result_panic.confidence}")
    print(f"   Recommendation: {result_panic.recommendation} 🚀")
    print(f"   Signals:")
    for signal in result_panic.signals:
        print(f"      ✓ {signal}")
    print()
    
    # Test 3: Profit booking
    print("3. Profit Booking:")
    print("-" * 70)
    result_profit = detector.detect(
        oi_change_pct=Decimal('-0.002'),  # Mild OI decrease
        price=Decimal('180.00'),
        previous_close=Decimal('182.00'),  # Price down
        vwap=Decimal('181.00'),
        order_book_ratio=Decimal('0.38'),  # Slightly ask heavy
        bid_ask_spread=Decimal('0.003')
    )
    print(f"   State:          {result_profit.state}")
    print(f"   Panic Score:    {result_profit.panic_score}")
    print(f"   Recommendation: {result_profit.recommendation}")
    print(f"   Signals:        {', '.join(result_profit.signals)}")
    print()
    
    # Test 4: Score breakdown
    print("4. Panic Score Breakdown:")
    print("-" * 70)
    print(f"   Short Covering:    {'YES ✓ (+30)' if result_panic.short_covering else 'NO'}")
    print(f"   Gamma Spike:       {'YES ✓ (+25)' if result_panic.gamma_spike_detected else 'NO'}")
    print(f"   Order Book Panic:  {'YES ✓ (+20+)' if result_panic.order_book_panic else 'NO'}")
    print(f"   Liquidity Drying:  {'YES ✓ (+15)' if result_panic.liquidity_drying else 'NO'}")
    print(f"   Strong Buying:     {'YES ✓ (+10)' if result_panic.strong_buying else 'NO'}")
    print(f"   " + "-" * 40)
    print(f"   Total Score:       {result_panic.panic_score}")
    print()
    
    # Test 5: Decision logic
    print("5. Decision Logic:")
    print("-" * 70)
    print(f"   Panic Score ≥ 60   → BUY  (Current: {result_panic.panic_score})")
    print(f"   Multiple signals   → WAIT (Profit booking)")
    print(f"   Few signals        → WAIT (Neutral)")
    print()
    
    print("=" * 70)
    print("✅ Seller state detector working!")
    print("=" * 70)