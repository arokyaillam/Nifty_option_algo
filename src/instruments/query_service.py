"""
Instrument Query Service - Direct Input
Input: Spot price + Expiry date → Get 10 options (ATM + 2ITM + 2OTM)
"""

from datetime import datetime
from typing import List, Dict
from sqlalchemy import select, and_
from sqlalchemy.ext.asyncio import AsyncSession
import logging

import sys
from pathlib import Path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from src.database.models import Instrument

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class InstrumentQueryService:
    """Direct query with spot + expiry"""
    
    def __init__(self, session: AsyncSession):
        self.session = session
    
    def get_required_strikes(self, spot_price: float) -> Dict[str, List[float]]:
        """Calculate ATM + 2ITM + 2OTM strikes"""
        atm = round(spot_price / 50) * 50
        
        # 5 strikes: 2ITM, 1ITM, ATM, 1OTM, 2OTM
        strikes = [
            atm - 100,  # 2 ITM
            atm - 50,   # 1 ITM
            atm,        # ATM
            atm + 50,   # 1 OTM
            atm + 100   # 2 OTM
        ]
        
        return {
            'strikes': strikes,
            'atm': atm
        }
    
    async def get_trading_options(
        self,
        spot_price: float,
        expiry_date: str  # YYYY-MM-DD format
    ) -> List[Instrument]:
        """
        Get 10 trading options
        
        Args:
            spot_price: Spot price
            expiry_date: Expiry in YYYY-MM-DD format
            
        Returns:
            10 instruments (5 CE + 5 PE)
        """
        # Parse expiry
        try:
            expiry = datetime.strptime(expiry_date, '%Y-%m-%d')
        except:
            logger.error(f"❌ Invalid date format: {expiry_date}")
            return []
        
        # Get strikes
        strikes_info = self.get_required_strikes(spot_price)
        strikes = strikes_info['strikes']
        atm = strikes_info['atm']
        
        logger.info(f"🎯 ATM: {atm}")
        logger.info(f"   Strikes: {strikes}")
        logger.info(f"   Expiry: {expiry_date}")
        
        # Query both CE and PE
        stmt = (
            select(Instrument)
            .where(
                and_(
                    Instrument.exchange == 'NSE_FO',
                    Instrument.expiry == expiry,
                    Instrument.strike.in_(strikes)
                )
            )
            .order_by(Instrument.option_type.desc(), Instrument.strike)  # CE first, then PE
        )
        
        result = await self.session.execute(stmt)
        instruments = list(result.scalars().all())
        
        ce_count = len([i for i in instruments if i.option_type == 'CE'])
        pe_count = len([i for i in instruments if i.option_type == 'PE'])
        
        logger.info(f"✅ Found {ce_count} CE + {pe_count} PE = {len(instruments)} total")
        
        return instruments
    
    async def get_instrument_keys(
        self,
        spot_price: float,
        expiry_date: str
    ) -> List[str]:
        """Get instrument keys"""
        instruments = await self.get_trading_options(spot_price, expiry_date)
        return [inst.instrument_key for inst in instruments]


# ========================
# CLI
# ========================
if __name__ == "__main__":
    import asyncio
    from src.database.engine import get_async_session
    
    async def main():
        print("=" * 70)
        print("Nifty Options Query - Direct Input")
        print("=" * 70)
        print()
        
        # Direct inputs
        spot = float(input("Enter Nifty spot price (e.g., 24500): "))
        expiry = input("Enter expiry date (YYYY-MM-DD, e.g., 2024-11-28): ").strip()
        
        print()
        print(f"Spot: {spot}")
        print(f"Expiry: {expiry}")
        print()
        
        async for session in get_async_session():
            service = InstrumentQueryService(session)
            
            # Get strikes
            strikes_info = service.get_required_strikes(spot)
            atm = strikes_info['atm']
            strikes = strikes_info['strikes']
            
            print(f"ATM Strike: {atm}")
            print(f"Strikes: {strikes}")
            print()
            
            # Get options
            instruments = await service.get_trading_options(spot, expiry)
            
            if not instruments:
                print("❌ No options found!")
                print("   Check if:")
                print("   1. Instruments are synced (run sync_service.py)")
                print("   2. Expiry date is correct format (YYYY-MM-DD)")
                print("   3. Expiry exists in database")
                return
            
            # Separate CE and PE
            ce_options = sorted([i for i in instruments if i.option_type == 'CE'], key=lambda x: x.strike)
            pe_options = sorted([i for i in instruments if i.option_type == 'PE'], key=lambda x: x.strike)
            
            print("Selected Options:")
            print("=" * 70)
            print(f"{'Expiry':<12} {'Strike':>8} {'Type':>6} {'Position':<10} {'Symbol':<30}")
            print("-" * 70)
            
            # Show CE
            for inst in ce_options:
                if inst.strike == atm:
                    pos = "ATM"
                elif inst.strike < atm:
                    itm_level = int((atm - inst.strike) / 50)
                    pos = f"{itm_level} ITM"
                else:
                    otm_level = int((inst.strike - atm) / 50)
                    pos = f"{otm_level} OTM"
                
                print(f"{inst.expiry.strftime('%Y-%m-%d'):<12} "
                      f"{inst.strike:8.0f} "
                      f"{inst.option_type:>6} "
                      f"{pos:<10} "
                      f"{inst.symbol:<30}")
            
            # Show PE
            for inst in pe_options:
                if inst.strike == atm:
                    pos = "ATM"
                elif inst.strike > atm:
                    itm_level = int((inst.strike - atm) / 50)
                    pos = f"{itm_level} ITM"
                else:
                    otm_level = int((atm - inst.strike) / 50)
                    pos = f"{otm_level} OTM"
                
                print(f"{inst.expiry.strftime('%Y-%m-%d'):<12} "
                      f"{inst.strike:8.0f} "
                      f"{inst.option_type:>6} "
                      f"{pos:<10} "
                      f"{inst.symbol:<30}")
            
            print("=" * 70)
            print()
            
            # Show keys
            keys = await service.get_instrument_keys(spot, expiry)
            
            print(f"Total: {len(keys)} instrument keys ({len(ce_options)} CE + {len(pe_options)} PE)")
            print()
            print("Instrument Keys:")
            print("-" * 70)
            for key in keys:
                print(f"  {key}")
            print("-" * 70)
            print()
            print("=" * 70)
            
            break
    
    asyncio.run(main())