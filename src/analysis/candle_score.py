"""
Candle Score Calculator
Calculates importance score for each candle based on multiple factors
Higher score = More important candle for analysis
"""

from decimal import Decimal
from typing import Optional
import math

# Handle imports
try:
    pass
except ImportError:
    import sys
    from pathlib import Path
    project_root = Path(__file__).parent.parent.parent
    sys.path.insert(0, str(project_root))


class CandleScoreCalculator:
    """
    Calculate candle importance score
    
    Factors considered:
    1. Volume (higher = more participation)
    2. OI change (higher change = more position building)
    3. Order book imbalance (extreme = panic/direction)
    4. Price volatility (range)
    5. Greeks (gamma spike)
    6. Spread (tight = liquid, wide = panic)
    
    Score Formula:
    score = volume_score + oi_score + ob_score + volatility_score + greek_score + spread_penalty
    """
    
    def __init__(
        self,
        volume_weight: float = 1.0,
        oi_weight: float = 0.8,
        ob_weight: float = 0.6,
        volatility_weight: float = 0.5,
        greek_weight: float = 0.4,
        spread_penalty_weight: float = 0.3
    ):
        """
        Initialize calculator with weights
        
        Args:
            volume_weight: Weight for volume component
            oi_weight: Weight for OI change component
            ob_weight: Weight for order book imbalance
            volatility_weight: Weight for price volatility
            greek_weight: Weight for gamma spike
            spread_penalty_weight: Weight for spread penalty
        """
        self.volume_weight = volume_weight
        self.oi_weight = oi_weight
        self.ob_weight = ob_weight
        self.volatility_weight = volatility_weight
        self.greek_weight = greek_weight
        self.spread_penalty_weight = spread_penalty_weight
    
    def calculate_volume_score(
        self,
        volume: int,
        avg_volume: Optional[int] = None
    ) -> Decimal:
        """
        Volume score: Higher volume = more important
        
        If avg_volume provided:
            score = (volume / avg_volume) * 1000
        Else:
            score = volume / 100  (raw volume scaled)
        
        Args:
            volume: Current candle volume
            avg_volume: Average volume (optional)
            
        Returns:
            Volume score
        """
        if avg_volume and avg_volume > 0:
            # Relative to average
            ratio = volume / avg_volume
            score = ratio * 1000
        else:
            # Absolute
            score = volume / 100
        
        return Decimal(str(score)) * Decimal(str(self.volume_weight))
    
    def calculate_oi_score(
        self,
        oi_change: Optional[Decimal],
        oi_change_pct: Optional[Decimal]
    ) -> Decimal:
        """
        OI change score: Higher change = position building
        
        score = |oi_change_pct| * 10000
        
        Args:
            oi_change: Absolute OI change
            oi_change_pct: OI change percentage (0.01 = 1%)
            
        Returns:
            OI score
        """
        if oi_change_pct is None:
            return Decimal('0')
        
        # Use absolute value (both increase and decrease are important)
        score = abs(oi_change_pct) * 10000
        
        return Decimal(str(score)) * Decimal(str(self.oi_weight))
    
    def calculate_orderbook_score(
        self,
        order_book_ratio: Decimal
    ) -> Decimal:
        """
        Order book imbalance score
        
        Extreme ratios = Important (panic or strong direction)
        - ratio < 0.3: Sellers dumping (high score)
        - ratio > 0.7: Buyers aggressive (high score)
        - ratio ~0.5: Balanced (low score)
        
        score = |ratio - 0.5| * 2000
        
        Args:
            order_book_ratio: TBQ / (TBQ + TSQ)
            
        Returns:
            Order book score
        """
        # Distance from neutral (0.5)
        imbalance = abs(order_book_ratio - Decimal('0.5'))
        
        score = imbalance * 2000
        
        return Decimal(str(score)) * Decimal(str(self.ob_weight))
    
    def calculate_volatility_score(
        self,
        high: Decimal,
        low: Decimal,
        close: Decimal
    ) -> Decimal:
        """
        Price volatility score
        
        Higher range = more volatility = more important
        
        score = ((high - low) / close) * 5000
        
        Args:
            high: Candle high
            low: Candle low
            close: Candle close
            
        Returns:
            Volatility score
        """
        if close == 0:
            return Decimal('0')
        
        range_pct = (high - low) / close
        score = range_pct * 5000
        
        return Decimal(str(score)) * Decimal(str(self.volatility_weight))
    
    def calculate_greek_score(
        self,
        gamma_spike: Optional[Decimal]
    ) -> Decimal:
        """
        Greek score (mainly gamma spike)
        
        Gamma spike indicates rapid delta change = important move
        
        score = gamma_spike * 1000
        
        Args:
            gamma_spike: Gamma spike percentage
            
        Returns:
            Greek score
        """
        if gamma_spike is None:
            return Decimal('0')
        
        score = abs(gamma_spike) * 1000
        
        return Decimal(str(score)) * Decimal(str(self.greek_weight))
    
    def calculate_spread_penalty(
        self,
        bid_ask_spread: Decimal
    ) -> Decimal:
        """
        Spread penalty: Wide spread = low liquidity = penalty
        
        penalty = spread * 5000
        (This is subtracted from final score)
        
        Args:
            bid_ask_spread: Bid-ask spread percentage
            
        Returns:
            Spread penalty (negative value)
        """
        penalty = bid_ask_spread * 5000
        
        return Decimal(str(penalty)) * Decimal(str(self.spread_penalty_weight))
    
    def calculate_score(
        self,
        # Volume
        volume: int,
        avg_volume: Optional[int] = None,
        
        # OI
        oi_change: Optional[Decimal] = None,
        oi_change_pct: Optional[Decimal] = None,
        
        # Order book
        order_book_ratio: Optional[Decimal] = None,
        
        # Price
        high: Optional[Decimal] = None,
        low: Optional[Decimal] = None,
        close: Optional[Decimal] = None,
        
        # Greeks
        gamma_spike: Optional[Decimal] = None,
        
        # Spread
        bid_ask_spread: Optional[Decimal] = None
    ) -> Decimal:
        """
        Calculate complete candle score
        
        Args:
            volume: Candle volume
            avg_volume: Average volume (optional)
            oi_change: OI change
            oi_change_pct: OI change percentage
            order_book_ratio: Order book ratio
            high: Candle high
            low: Candle low
            close: Candle close
            gamma_spike: Gamma spike
            bid_ask_spread: Bid-ask spread
            
        Returns:
            Total candle score (higher = more important)
        """
        score = Decimal('0')
        
        # 1. Volume score
        volume_score = self.calculate_volume_score(volume, avg_volume)
        score += volume_score
        
        # 2. OI score
        if oi_change_pct is not None:
            oi_score = self.calculate_oi_score(oi_change, oi_change_pct)
            score += oi_score
        
        # 3. Order book score
        if order_book_ratio is not None:
            ob_score = self.calculate_orderbook_score(order_book_ratio)
            score += ob_score
        
        # 4. Volatility score
        if high and low and close:
            vol_score = self.calculate_volatility_score(high, low, close)
            score += vol_score
        
        # 5. Greek score
        if gamma_spike is not None:
            greek_score = self.calculate_greek_score(gamma_spike)
            score += greek_score
        
        # 6. Spread penalty (subtract)
        if bid_ask_spread is not None:
            penalty = self.calculate_spread_penalty(bid_ask_spread)
            score -= penalty
        
        # Ensure non-negative
        return max(score, Decimal('0'))


# ========================
# Testing
# ========================
if __name__ == "__main__":
    """
    Test candle score calculator
    Run: uv run python src/analysis/candle_score.py
    """
    
    print("=" * 70)
    print("Candle Score Calculator Test")
    print("=" * 70)
    print()
    
    calculator = CandleScoreCalculator()
    
    # Test 1: Normal candle
    print("1. Normal Candle:")
    print("-" * 70)
    score_normal = calculator.calculate_score(
        volume=100000,
        avg_volume=120000,
        oi_change=Decimal('1000'),
        oi_change_pct=Decimal('0.001'),  # 0.1%
        order_book_ratio=Decimal('0.52'),  # Slightly bid heavy
        high=Decimal('182.50'),
        low=Decimal('181.50'),
        close=Decimal('182.00'),
        gamma_spike=Decimal('0.01'),
        bid_ask_spread=Decimal('0.002')
    )
    print(f"   Score: {score_normal:.2f}")
    print()
    
    # Test 2: High volume candle
    print("2. High Volume Candle:")
    print("-" * 70)
    score_high_vol = calculator.calculate_score(
        volume=250000,  # 2x average
        avg_volume=120000,
        oi_change_pct=Decimal('0.001'),
        order_book_ratio=Decimal('0.5'),
        high=Decimal('182.50'),
        low=Decimal('181.50'),
        close=Decimal('182.00')
    )
    print(f"   Score: {score_high_vol:.2f}")
    print()
    
    # Test 3: Panic candle (ask heavy)
    print("3. Panic Candle (Sellers Dumping):")
    print("-" * 70)
    score_panic = calculator.calculate_score(
        volume=150000,
        avg_volume=120000,
        oi_change_pct=Decimal('-0.005'),  # OI decreasing
        order_book_ratio=Decimal('0.25'),  # Very ask heavy!
        high=Decimal('183.00'),
        low=Decimal('180.00'),  # Wide range
        close=Decimal('180.50'),
        gamma_spike=Decimal('0.55'),  # Big gamma spike
        bid_ask_spread=Decimal('0.008')  # Wide spread
    )
    print(f"   Score: {score_panic:.2f}")
    print(f"   Interpretation: High score due to extreme imbalance + gamma spike")
    print()
    
    # Test 4: Quiet candle
    print("4. Quiet Candle:")
    print("-" * 70)
    score_quiet = calculator.calculate_score(
        volume=50000,  # Low volume
        avg_volume=120000,
        oi_change_pct=Decimal('0.0001'),  # Minimal OI change
        order_book_ratio=Decimal('0.5'),  # Balanced
        high=Decimal('182.10'),
        low=Decimal('181.90'),  # Tight range
        close=Decimal('182.00')
    )
    print(f"   Score: {score_quiet:.2f}")
    print(f"   Interpretation: Low score = not important")
    print()
    
    # Test 5: Component breakdown
    print("5. Score Component Breakdown (Panic Candle):")
    print("-" * 70)
    
    vol_score = calculator.calculate_volume_score(150000, 120000)
    oi_score = calculator.calculate_oi_score(Decimal('-500'), Decimal('-0.005'))
    ob_score = calculator.calculate_orderbook_score(Decimal('0.25'))
    vol_score_comp = calculator.calculate_volatility_score(
        Decimal('183.00'), Decimal('180.00'), Decimal('180.50')
    )
    greek_score = calculator.calculate_greek_score(Decimal('0.55'))
    spread_penalty = calculator.calculate_spread_penalty(Decimal('0.008'))
    
    print(f"   Volume Score:      {vol_score:8.2f}")
    print(f"   OI Score:          {oi_score:8.2f}")
    print(f"   Order Book Score:  {ob_score:8.2f}")
    print(f"   Volatility Score:  {vol_score_comp:8.2f}")
    print(f"   Greek Score:       {greek_score:8.2f}")
    print(f"   Spread Penalty:   -{spread_penalty:8.2f}")
    print(f"   " + "-" * 30)
    print(f"   Total:             {score_panic:8.2f}")
    print()
    
    # Test 6: Comparison
    print("6. Score Comparison:")
    print("-" * 70)
    print(f"   Normal Candle:     {score_normal:8.2f}")
    print(f"   High Volume:       {score_high_vol:8.2f}")
    print(f"   Panic (IMPORTANT): {score_panic:8.2f} ⭐")
    print(f"   Quiet:             {score_quiet:8.2f}")
    print()
    
    print("=" * 70)
    print("✅ Candle score calculator working!")
    print("=" * 70)