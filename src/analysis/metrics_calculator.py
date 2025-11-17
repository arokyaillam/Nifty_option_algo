"""
Metrics Calculator
Calculate various metrics from tick and candle data
"""

from decimal import Decimal
from typing import List, Optional, Tuple
from dataclasses import dataclass

# Handle imports
try:
    pass
except ImportError:
    import sys
    from pathlib import Path
    project_root = Path(__file__).parent.parent.parent
    sys.path.insert(0, str(project_root))


@dataclass
class VWAPResult:
    """VWAP calculation result"""
    vwap: Decimal
    deviation: Decimal  # (price - vwap) / vwap
    deviation_pct: Decimal  # deviation * 100


class MetricsCalculator:
    """
    Calculate various trading metrics
    
    Metrics:
    1. VWAP (Volume Weighted Average Price)
    2. Price changes
    3. OI changes
    4. Straddle analysis
    5. Greek averages
    """
    
    @staticmethod
    def calculate_vwap(
        prices: List[Decimal],
        quantities: List[int]
    ) -> Optional[Decimal]:
        """
        Calculate VWAP: Σ(price * quantity) / Σ(quantity)
        
        Args:
            prices: List of prices
            quantities: List of quantities (volumes)
            
        Returns:
            VWAP or None if no data
        """
        if not prices or not quantities or len(prices) != len(quantities):
            return None
        
        total_pq = sum(
            float(prices[i]) * quantities[i]
            for i in range(len(prices))
        )
        
        total_q = sum(quantities)
        
        if total_q == 0:
            return None
        
        return Decimal(str(total_pq / total_q))
    
    @staticmethod
    def calculate_vwap_with_deviation(
        current_price: Decimal,
        prices: List[Decimal],
        quantities: List[int]
    ) -> Optional[VWAPResult]:
        """
        Calculate VWAP and deviation from current price
        
        Args:
            current_price: Current price (LTP)
            prices: Historical prices
            quantities: Quantities
            
        Returns:
            VWAPResult with VWAP and deviation
        """
        vwap = MetricsCalculator.calculate_vwap(prices, quantities)
        
        if vwap is None or vwap == 0:
            return None
        
        deviation = (current_price - vwap) / vwap
        deviation_pct = deviation * 100
        
        return VWAPResult(
            vwap=vwap,
            deviation=deviation,
            deviation_pct=deviation_pct
        )
    
    @staticmethod
    def calculate_price_change(
        current_price: Decimal,
        previous_price: Decimal
    ) -> Tuple[Decimal, Decimal]:
        """
        Calculate price change
        
        Args:
            current_price: Current price
            previous_price: Previous price
            
        Returns:
            (change, change_pct) tuple
        """
        if previous_price == 0:
            return Decimal('0'), Decimal('0')
        
        change = current_price - previous_price
        change_pct = change / previous_price
        
        return change, change_pct
    
    @staticmethod
    def calculate_oi_change(
        current_oi: int,
        previous_oi: int
    ) -> Tuple[Decimal, Decimal]:
        """
        Calculate OI change
        
        Args:
            current_oi: Current OI
            previous_oi: Previous OI
            
        Returns:
            (change, change_pct) tuple
        """
        if previous_oi == 0:
            return Decimal('0'), Decimal('0')
        
        change = Decimal(current_oi - previous_oi)
        change_pct = change / Decimal(previous_oi)
        
        return change, change_pct
    
    @staticmethod
    def calculate_average_greek(
        greek_values: List[float]
    ) -> Optional[Decimal]:
        """
        Calculate average of Greek values
        
        Args:
            greek_values: List of Greek values (delta, gamma, etc.)
            
        Returns:
            Average or None if no data
        """
        if not greek_values:
            return None
        
        avg = sum(greek_values) / len(greek_values)
        return Decimal(str(avg))
    
    @staticmethod
    def calculate_gamma_spike(
        current_gamma: Optional[float],
        previous_gamma: Optional[float]
    ) -> Optional[Decimal]:
        """
        Calculate gamma spike percentage
        
        Gamma spike = (current - previous) / previous
        
        Args:
            current_gamma: Current gamma
            previous_gamma: Previous gamma
            
        Returns:
            Gamma spike percentage or None
        """
        if current_gamma is None or previous_gamma is None:
            return None
        
        if previous_gamma == 0:
            return None
        
        spike = (current_gamma - previous_gamma) / abs(previous_gamma)
        return Decimal(str(spike))
    
    @staticmethod
    def calculate_straddle_price(
        ce_price: Decimal,
        pe_price: Decimal
    ) -> Decimal:
        """
        Calculate straddle price: CE + PE
        
        Args:
            ce_price: Call option price
            pe_price: Put option price
            
        Returns:
            Straddle price
        """
        return ce_price + pe_price
    
    @staticmethod
    def calculate_straddle_deviation(
        current_straddle: Decimal,
        straddle_vwap: Decimal
    ) -> Optional[Decimal]:
        """
        Calculate straddle deviation from VWAP
        
        Args:
            current_straddle: Current straddle price
            straddle_vwap: Straddle VWAP
            
        Returns:
            Deviation or None
        """
        if straddle_vwap == 0:
            return None
        
        deviation = (current_straddle - straddle_vwap) / straddle_vwap
        return deviation
    
    @staticmethod
    def calculate_weighted_average(
        values: List[Decimal],
        weights: List[int]
    ) -> Optional[Decimal]:
        """
        Calculate weighted average
        
        Args:
            values: List of values
            weights: List of weights
            
        Returns:
            Weighted average or None
        """
        if not values or not weights or len(values) != len(weights):
            return None
        
        total_weighted = sum(
            float(values[i]) * weights[i]
            for i in range(len(values))
        )
        
        total_weight = sum(weights)
        
        if total_weight == 0:
            return None
        
        return Decimal(str(total_weighted / total_weight))
    
    @staticmethod
    def calculate_percentage_change(
        old_value: Decimal,
        new_value: Decimal
    ) -> Decimal:
        """
        Calculate percentage change: (new - old) / old * 100
        
        Args:
            old_value: Old value
            new_value: New value
            
        Returns:
            Percentage change
        """
        if old_value == 0:
            return Decimal('0')
        
        return ((new_value - old_value) / old_value) * 100
    
    @staticmethod
    def calculate_range_percentage(
        high: Decimal,
        low: Decimal,
        close: Decimal
    ) -> Decimal:
        """
        Calculate range as percentage of close
        
        range_pct = (high - low) / close * 100
        
        Args:
            high: High price
            low: Low price
            close: Close price
            
        Returns:
            Range percentage
        """
        if close == 0:
            return Decimal('0')
        
        return ((high - low) / close) * 100


# ========================
# Testing
# ========================
if __name__ == "__main__":
    """
    Test metrics calculator
    Run: uv run python src/analysis/metrics_calculator.py
    """
    
    print("=" * 70)
    print("Metrics Calculator Test")
    print("=" * 70)
    print()
    
    # Test 1: VWAP
    print("1. VWAP Calculation:")
    print("-" * 70)
    
    prices = [
        Decimal('182.00'), Decimal('182.50'), Decimal('183.00'),
        Decimal('182.75'), Decimal('182.25')
    ]
    quantities = [1000, 1500, 800, 1200, 900]
    
    vwap = MetricsCalculator.calculate_vwap(prices, quantities)
    print(f"   Prices:    {[float(p) for p in prices]}")
    print(f"   Volumes:   {quantities}")
    print(f"   VWAP:      {vwap}")
    print()
    
    # Test 2: VWAP Deviation
    print("2. VWAP Deviation:")
    print("-" * 70)
    
    current_price = Decimal('183.50')
    vwap_result = MetricsCalculator.calculate_vwap_with_deviation(
        current_price, prices, quantities
    )
    
    if vwap_result:
        print(f"   Current Price: {current_price}")
        print(f"   VWAP:          {vwap_result.vwap}")
        print(f"   Deviation:     {vwap_result.deviation:.6f}")
        print(f"   Deviation %:   {vwap_result.deviation_pct:.2f}%")
        
        if vwap_result.deviation > 0:
            print(f"   Status:        🟢 Price above VWAP (bullish)")
        else:
            print(f"   Status:        🔴 Price below VWAP (bearish)")
    print()
    
    # Test 3: Price Change
    print("3. Price Change:")
    print("-" * 70)
    
    prev_price = Decimal('180.00')
    curr_price = Decimal('185.00')
    
    change, change_pct = MetricsCalculator.calculate_price_change(
        curr_price, prev_price
    )
    
    print(f"   Previous: {prev_price}")
    print(f"   Current:  {curr_price}")
    print(f"   Change:   {change} ({change_pct*100:.2f}%)")
    print()
    
    # Test 4: OI Change
    print("4. OI Change:")
    print("-" * 70)
    
    prev_oi = 8000000
    curr_oi = 7950000
    
    oi_change, oi_change_pct = MetricsCalculator.calculate_oi_change(
        curr_oi, prev_oi
    )
    
    print(f"   Previous OI: {prev_oi:,}")
    print(f"   Current OI:  {curr_oi:,}")
    print(f"   Change:      {oi_change:,} ({oi_change_pct*100:.2f}%)")
    
    if float(oi_change_pct) < -0.003:
        print(f"   Status:      ⚠️ Significant OI decrease (SHORT COVERING?)")
    print()
    
    # Test 5: Average Greeks
    print("5. Average Greek Calculation:")
    print("-" * 70)
    
    delta_values = [0.45, 0.46, 0.44, 0.45, 0.47]
    avg_delta = MetricsCalculator.calculate_average_greek(delta_values)
    
    print(f"   Delta values: {delta_values}")
    print(f"   Average:      {avg_delta}")
    print()
    
    # Test 6: Gamma Spike
    print("6. Gamma Spike Detection:")
    print("-" * 70)
    
    prev_gamma = 0.0007
    curr_gamma = 0.0012
    
    gamma_spike = MetricsCalculator.calculate_gamma_spike(curr_gamma, prev_gamma)
    
    print(f"   Previous Gamma: {prev_gamma}")
    print(f"   Current Gamma:  {curr_gamma}")
    print(f"   Spike:          {gamma_spike*100:.2f}%")
    
    if gamma_spike and float(gamma_spike) > 0.30:
        print(f"   Status:         🚨 GAMMA SPIKE DETECTED!")
    print()
    
    # Test 7: Straddle
    print("7. Straddle Calculation:")
    print("-" * 70)
    
    ce_price = Decimal('185.50')
    pe_price = Decimal('180.00')
    
    straddle = MetricsCalculator.calculate_straddle_price(ce_price, pe_price)
    
    print(f"   CE Price:       {ce_price}")
    print(f"   PE Price:       {pe_price}")
    print(f"   Straddle Price: {straddle}")
    print()
    
    # Test 8: Range Percentage
    print("8. Range Percentage:")
    print("-" * 70)
    
    high = Decimal('185.00')
    low = Decimal('180.00')
    close = Decimal('182.50')
    
    range_pct = MetricsCalculator.calculate_range_percentage(high, low, close)
    
    print(f"   High:       {high}")
    print(f"   Low:        {low}")
    print(f"   Close:      {close}")
    print(f"   Range:      {range_pct:.2f}%")
    
    if float(range_pct) > 2:
        print(f"   Status:     🔥 High volatility candle")
    print()
    
    print("=" * 70)
    print("✅ Metrics calculator working!")
    print("=" * 70)