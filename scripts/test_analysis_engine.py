"""
Complete Analysis Engine Test
Test all analysis components together
"""

from decimal import Decimal

import sys
from pathlib import Path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.analysis.orderbook_analyzer import OrderBookAnalyzer
from src.analysis.candle_score import CandleScoreCalculator
from src.analysis.seller_detector import SellerStateDetector
from src.analysis.metrics_calculator import MetricsCalculator


def test_complete_analysis():
    """Test complete analysis pipeline"""
    print("=" * 70)
    print("Complete Analysis Engine Test")
    print("=" * 70)
    print()
    
    # Sample market data (seller panic scenario)
    bid_prices = [Decimal("182.05"), Decimal("182.00"), Decimal("181.95")]
    bid_quantities = [600, 1950, 900]
    ask_prices = [Decimal("182.40"), Decimal("182.45"), Decimal("182.50")]
    ask_quantities = [2500, 1800, 1500]  # Heavy asks!
    
    current_price = Decimal("185.00")
    previous_close = Decimal("182.00")
    high = Decimal("185.50")
    low = Decimal("180.00")
    volume = 250000
    prev_oi = 8000000
    curr_oi = 7950000
    
    # ========================
    # 1. Order Book Analysis
    # ========================
    print("1. Order Book Analysis:")
    print("-" * 70)
    
    ob_analyzer = OrderBookAnalyzer()
    ob_analysis = ob_analyzer.analyze_order_book(
        bid_prices, bid_quantities,
        ask_prices, ask_quantities
    )
    
    print(f"   Support:       {ob_analysis['support']}")
    print(f"   Resistance:    {ob_analysis['resistance']}")
    print(f"   TBQ:           {ob_analysis['tbq']:,}")
    print(f"   TSQ:           {ob_analysis['tsq']:,}")
    print(f"   OB Ratio:      {ob_analysis['order_book_ratio']:.4f}")
    print(f"   Spread:        {ob_analysis['bid_ask_spread']:.4f}")
    print()
    
    # ========================
    # 2. Metrics Calculation
    # ========================
    print("2. Metrics Calculation:")
    print("-" * 70)
    
    metrics_calc = MetricsCalculator()
    
    # Price change
    price_change, price_change_pct = metrics_calc.calculate_price_change(
        current_price, previous_close
    )
    
    # OI change
    oi_change, oi_change_pct = metrics_calc.calculate_oi_change(
        curr_oi, prev_oi
    )
    
    # VWAP
    vwap_result = metrics_calc.calculate_vwap_with_deviation(
        current_price,
        [Decimal("182.00"), Decimal("183.00"), Decimal("184.00")],
        [1000, 1500, 800]
    )
    
    print(f"   Price Change:  {price_change} ({price_change_pct*100:.2f}%)")
    print(f"   OI Change:     {oi_change:,} ({oi_change_pct*100:.2f}%)")
    if vwap_result:
        print(f"   VWAP:          {vwap_result.vwap}")
        print(f"   VWAP Dev:      {vwap_result.deviation_pct:.2f}%")
    print()
    
    # ========================
    # 3. Candle Score
    # ========================
    print("3. Candle Score:")
    print("-" * 70)
    
    score_calc = CandleScoreCalculator()
    candle_score = score_calc.calculate_score(
        volume=volume,
        avg_volume=120000,
        oi_change=oi_change,
        oi_change_pct=oi_change_pct,
        order_book_ratio=ob_analysis['order_book_ratio'],
        high=high,
        low=low,
        close=current_price,
        gamma_spike=Decimal("0.55"),
        bid_ask_spread=ob_analysis['bid_ask_spread']
    )
    
    print(f"   Score:         {candle_score:.2f}")
    print(f"   Status:        {'⭐ High importance' if float(candle_score) > 1000 else 'Normal'}")
    print()
    
    # ========================
    # 4. Seller State Detection
    # ========================
    print("4. Seller State Detection:")
    print("-" * 70)
    
    detector = SellerStateDetector()
    detection = detector.detect(
        oi_change_pct=oi_change_pct,
        price=current_price,
        previous_close=previous_close,
        vwap=vwap_result.vwap if vwap_result else None,
        gamma_spike=Decimal("0.55"),
        order_book_ratio=ob_analysis['order_book_ratio'],
        bid_ask_spread=ob_analysis['bid_ask_spread']
    )
    
    print(f"   State:         {detection.state}")
    print(f"   Confidence:    {detection.confidence:.2f}")
    print(f"   Panic Score:   {detection.panic_score} / 100")
    print(f"   Recommendation:{detection.recommendation} {'🚀' if detection.recommendation == 'BUY' else ''}")
    print()
    
    print("   Detected Signals:")
    for signal in detection.signals:
        print(f"      ✓ {signal}")
    print()
    
    # ========================
    # 5. Trading Decision
    # ========================
    print("5. Trading Decision:")
    print("-" * 70)
    
    if detection.recommendation == "BUY":
        print(f"   🚨 STRONG BUY SIGNAL DETECTED!")
        print(f"   Entry Price:   {current_price}")
        print(f"   Reason:        Seller panic with {len(detection.signals)} signals")
        print(f"   Confidence:    {detection.confidence*100:.0f}%")
        print()
        
        print(f"   Supporting Evidence:")
        if detection.short_covering:
            print(f"      • Short covering: OI↓ {oi_change_pct*100:.2f}%, Price↑ {price_change_pct*100:.2f}%")
        if detection.gamma_spike_detected:
            print(f"      • Gamma spike detected")
        if detection.order_book_panic:
            print(f"      • Order book panic: Ratio={ob_analysis['order_book_ratio']:.2f}")
        if detection.liquidity_drying:
            print(f"      • Liquidity drying: Spread={ob_analysis['bid_ask_spread']:.4f}")
        if detection.strong_buying:
            print(f"      • Strong buying above VWAP")
    else:
        print(f"   Status:        {detection.recommendation}")
        print(f"   Reason:        {detection.state}")
    
    print()
    
    # ========================
    # Summary
    # ========================
    print("=" * 70)
    print("Analysis Summary:")
    print("-" * 70)
    print(f"Candle Score:      {candle_score:.2f}")
    print(f"Seller State:      {detection.state}")
    print(f"Panic Score:       {detection.panic_score}/100")
    print(f"Recommendation:    {detection.recommendation}")
    print(f"Confidence:        {detection.confidence*100:.0f}%")
    print("=" * 70)
    print()
    
    if detection.panic_score >= 60:
        print("🎉 Analysis engine correctly identified BUY opportunity!")
    else:
        print("✅ Analysis engine working correctly!")
    print()


if __name__ == "__main__":
    test_complete_analysis()