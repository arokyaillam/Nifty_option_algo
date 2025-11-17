"""
Order Book Analyzer
Analyzes 30-level order book for support/resistance and market depth
"""

from decimal import Decimal
from typing import List, Tuple, Optional
from dataclasses import dataclass
import statistics

# Handle imports
try:
    pass
except ImportError:
    import sys
    from pathlib import Path
    project_root = Path(__file__).parent.parent.parent
    sys.path.insert(0, str(project_root))


@dataclass
class OrderBookLevel:
    """Single order book level"""
    price: Decimal
    quantity: int


@dataclass
class SupportResistance:
    """Support and resistance levels from order book"""
    # Top 3 support levels (bids with highest quantity)
    support_levels: List[Tuple[Decimal, int]]  # [(price, qty), ...]
    support_avg: Decimal
    
    # Top 3 resistance levels (asks with highest quantity)
    resistance_levels: List[Tuple[Decimal, int]]
    resistance_avg: Decimal


class OrderBookAnalyzer:
    """
    Analyze order book for trading signals
    
    Key Functions:
    1. Find top 3 bid/ask levels by quantity (support/resistance)
    2. Calculate TBQ/TSQ (total bid/sell quantity)
    3. Detect big orders (whale detection)
    4. Calculate bid-ask spread
    """
    
    def calculate_sup_res(
        self,
        bid_prices: List[Decimal],
        bid_quantities: List[int],
        ask_prices: List[Decimal],
        ask_quantities: List[int]
    ) -> SupportResistance:
        """
        Calculate support and resistance from order book
        
        Support = Top 3 bid levels with highest quantity
        Resistance = Top 3 ask levels with highest quantity
        
        Args:
            bid_prices: List of bid prices (30 levels)
            bid_quantities: List of bid quantities (30 levels)
            ask_prices: List of ask prices (30 levels)
            ask_quantities: List of ask quantities (30 levels)
            
        Returns:
            SupportResistance with top 3 levels and averages
        """
        # Create bid levels
        bid_levels = [
            OrderBookLevel(price=bid_prices[i], quantity=bid_quantities[i])
            for i in range(min(len(bid_prices), len(bid_quantities)))
        ]
        
        # Create ask levels
        ask_levels = [
            OrderBookLevel(price=ask_prices[i], quantity=ask_quantities[i])
            for i in range(min(len(ask_prices), len(ask_quantities)))
        ]
        
        # Sort by quantity (descending) and get top 3
        top_3_bids = sorted(bid_levels, key=lambda x: x.quantity, reverse=True)[:3]
        top_3_asks = sorted(ask_levels, key=lambda x: x.quantity, reverse=True)[:3]
        
        # Ensure we have at least 3 levels (pad with zeros if needed)
        while len(top_3_bids) < 3:
            top_3_bids.append(OrderBookLevel(price=Decimal('0'), quantity=0))
        
        while len(top_3_asks) < 3:
            top_3_asks.append(OrderBookLevel(price=Decimal('0'), quantity=0))
        
        # Extract as tuples
        support_levels = [(level.price, level.quantity) for level in top_3_bids]
        resistance_levels = [(level.price, level.quantity) for level in top_3_asks]
        
        # Calculate averages (only non-zero prices)
        support_prices = [level.price for level in top_3_bids if level.price > 0]
        resistance_prices = [level.price for level in top_3_asks if level.price > 0]
        
        support_avg = (
            sum(support_prices) / len(support_prices)
            if support_prices else Decimal('0')
        )
        
        resistance_avg = (
            sum(resistance_prices) / len(resistance_prices)
            if resistance_prices else Decimal('0')
        )
        
        return SupportResistance(
            support_levels=support_levels,
            support_avg=support_avg,
            resistance_levels=resistance_levels,
            resistance_avg=resistance_avg
        )
    
    def calculate_tbq_tsq(
        self,
        bid_quantities: List[int],
        ask_quantities: List[int]
    ) -> Tuple[int, int]:
        """
        Calculate Total Bid Quantity and Total Sell Quantity
        
        Args:
            bid_quantities: List of bid quantities
            ask_quantities: List of ask quantities
            
        Returns:
            (TBQ, TSQ) tuple
        """
        tbq = sum(bid_quantities)
        tsq = sum(ask_quantities)
        return tbq, tsq
    
    def calculate_order_book_ratio(self, tbq: int, tsq: int) -> Decimal:
        """
        Calculate order book ratio: TBQ / (TBQ + TSQ)
        
        Ratio interpretation:
        - > 0.6: Bid heavy (buyers aggressive)
        - < 0.4: Ask heavy (sellers dumping) → PANIC SIGNAL
        - ~0.5: Balanced
        
        Args:
            tbq: Total Bid Quantity
            tsq: Total Sell Quantity
            
        Returns:
            Order book ratio (0 to 1)
        """
        total = tbq + tsq
        if total == 0:
            return Decimal('0.5')  # Neutral if no data
        
        return Decimal(tbq) / Decimal(total)
    
    def detect_big_quantities(
        self,
        quantities: List[int],
        threshold_multiplier: float = 5.0
    ) -> int:
        """
        Detect big (whale) orders
        
        Big order = quantity > median * threshold_multiplier
        
        Args:
            quantities: List of quantities
            threshold_multiplier: Multiplier for median (default: 5x)
            
        Returns:
            Count of big orders
        """
        if not quantities:
            return 0
        
        # Calculate median
        median_qty = statistics.median(quantities)
        
        # Threshold
        threshold = median_qty * threshold_multiplier
        
        # Count quantities above threshold
        big_count = sum(1 for qty in quantities if qty > threshold)
        
        return big_count
    
    def calculate_spread(
        self,
        best_bid: Decimal,
        best_ask: Decimal
    ) -> Decimal:
        """
        Calculate bid-ask spread
        
        Spread = (best_ask - best_bid) / best_bid
        
        Args:
            best_bid: Best bid price
            best_ask: Best ask price
            
        Returns:
            Spread as decimal (e.g., 0.0019 = 0.19%)
        """
        if best_bid == 0:
            return Decimal('0')
        
        return (best_ask - best_bid) / best_bid
    
    def analyze_order_book(
        self,
        bid_prices: List[Decimal],
        bid_quantities: List[int],
        ask_prices: List[Decimal],
        ask_quantities: List[int]
    ) -> dict:
        """
        Complete order book analysis
        
        Returns all metrics in one call
        
        Args:
            bid_prices: Bid prices (30 levels)
            bid_quantities: Bid quantities
            ask_prices: Ask prices (30 levels)
            ask_quantities: Ask quantities
            
        Returns:
            Dictionary with all order book metrics
        """
        # Support/Resistance
        sup_res = self.calculate_sup_res(
            bid_prices, bid_quantities,
            ask_prices, ask_quantities
        )
        
        # TBQ/TSQ
        tbq, tsq = self.calculate_tbq_tsq(bid_quantities, ask_quantities)
        
        # Order book ratio
        ob_ratio = self.calculate_order_book_ratio(tbq, tsq)
        
        # Spread
        best_bid = bid_prices[0] if bid_prices else Decimal('0')
        best_ask = ask_prices[0] if ask_prices else Decimal('0')
        spread = self.calculate_spread(best_bid, best_ask)
        
        # Big quantities
        big_bid_count = self.detect_big_quantities(bid_quantities)
        big_ask_count = self.detect_big_quantities(ask_quantities)
        
        return {
            # Support
            "support_level_1": sup_res.support_levels[0][0],
            "support_qty_1": sup_res.support_levels[0][1],
            "support_level_2": sup_res.support_levels[1][0],
            "support_qty_2": sup_res.support_levels[1][1],
            "support_level_3": sup_res.support_levels[2][0],
            "support_qty_3": sup_res.support_levels[2][1],
            "support": sup_res.support_avg,
            
            # Resistance
            "resistance_level_1": sup_res.resistance_levels[0][0],
            "resistance_qty_1": sup_res.resistance_levels[0][1],
            "resistance_level_2": sup_res.resistance_levels[1][0],
            "resistance_qty_2": sup_res.resistance_levels[1][1],
            "resistance_level_3": sup_res.resistance_levels[2][0],
            "resistance_qty_3": sup_res.resistance_levels[2][1],
            "resistance": sup_res.resistance_avg,
            
            # Order book metrics
            "tbq": tbq,
            "tsq": tsq,
            "order_book_ratio": ob_ratio,
            "bid_ask_spread": spread,
            "big_bid_count": big_bid_count,
            "big_ask_count": big_ask_count,
        }


# ========================
# Testing
# ========================
if __name__ == "__main__":
    """
    Test order book analyzer
    Run: uv run python src/analysis/orderbook_analyzer.py
    """
    
    print("=" * 70)
    print("Order Book Analyzer Test")
    print("=" * 70)
    print()
    
    # Sample order book (from your Upstox data)
    bid_prices = [
        Decimal("182.05"), Decimal("182.00"), Decimal("181.95"),
        Decimal("181.90"), Decimal("181.85"), Decimal("181.80")
    ]
    bid_quantities = [600, 1950, 900, 1350, 900, 1200]
    
    ask_prices = [
        Decimal("182.40"), Decimal("182.45"), Decimal("182.50"),
        Decimal("182.55"), Decimal("182.60"), Decimal("182.65")
    ]
    ask_quantities = [750, 675, 1800, 1200, 750, 1275]
    
    analyzer = OrderBookAnalyzer()
    
    # Test 1: Support/Resistance
    print("1. Support & Resistance Analysis:")
    print("-" * 70)
    sup_res = analyzer.calculate_sup_res(
        bid_prices, bid_quantities,
        ask_prices, ask_quantities
    )
    
    print("   Support (Top 3 by Quantity):")
    for i, (price, qty) in enumerate(sup_res.support_levels, 1):
        print(f"      {i}. {price} x {qty}")
    print(f"   Average Support: {sup_res.support_avg}")
    print()
    
    print("   Resistance (Top 3 by Quantity):")
    for i, (price, qty) in enumerate(sup_res.resistance_levels, 1):
        print(f"      {i}. {price} x {qty}")
    print(f"   Average Resistance: {sup_res.resistance_avg}")
    print()
    
    # Test 2: TBQ/TSQ
    print("2. Total Bid/Sell Quantity:")
    print("-" * 70)
    tbq, tsq = analyzer.calculate_tbq_tsq(bid_quantities, ask_quantities)
    print(f"   TBQ: {tbq:,}")
    print(f"   TSQ: {tsq:,}")
    print()
    
    # Test 3: Order Book Ratio
    print("3. Order Book Ratio:")
    print("-" * 70)
    ratio = analyzer.calculate_order_book_ratio(tbq, tsq)
    print(f"   Ratio: {ratio:.4f}")
    if ratio > 0.6:
        print(f"   Status: 🟢 Bid heavy (buyers aggressive)")
    elif ratio < 0.4:
        print(f"   Status: 🔴 Ask heavy (sellers dumping) - PANIC!")
    else:
        print(f"   Status: 🟡 Balanced")
    print()
    
    # Test 4: Spread
    print("4. Bid-Ask Spread:")
    print("-" * 70)
    spread = analyzer.calculate_spread(bid_prices[0], ask_prices[0])
    print(f"   Best Bid: {bid_prices[0]}")
    print(f"   Best Ask: {ask_prices[0]}")
    print(f"   Spread: {spread:.4f} ({float(spread)*100:.2f}%)")
    print()
    
    # Test 5: Big Quantities
    print("5. Whale Detection:")
    print("-" * 70)
    big_bids = analyzer.detect_big_quantities(bid_quantities)
    big_asks = analyzer.detect_big_quantities(ask_quantities)
    print(f"   Big Bids: {big_bids}")
    print(f"   Big Asks: {big_asks}")
    print()
    
    # Test 6: Complete Analysis
    print("6. Complete Analysis:")
    print("-" * 70)
    analysis = analyzer.analyze_order_book(
        bid_prices, bid_quantities,
        ask_prices, ask_quantities
    )
    
    print(f"   Support:     {analysis['support']}")
    print(f"   Resistance:  {analysis['resistance']}")
    print(f"   OB Ratio:    {analysis['order_book_ratio']:.4f}")
    print(f"   Spread:      {analysis['bid_ask_spread']:.4f}")
    print()
    
    print("=" * 70)
    print("✅ Order book analyzer working!")
    print("=" * 70)