"""
Instrument Sync Service
Monthly sync: Fetch ALL option contracts and store in PostgreSQL
"""

import requests
from datetime import datetime, timedelta
from typing import List, Dict, Optional
from sqlalchemy import select, func, delete
from sqlalchemy.ext.asyncio import AsyncSession
import logging

import sys
from pathlib import Path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from src.database.engine import get_async_session
from src.database.models import Instrument

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class InstrumentSyncService:
    """
    Monthly sync service
    
    Workflow:
    1. Check if instruments are older than 30 days
    2. If yes, fetch ALL Nifty option contracts from Upstox
    3. Store in PostgreSQL
    4. Daily usage just queries database (no API calls)
    """
    
    BASE_URL = "https://api.upstox.com/v2"
    REFRESH_DAYS = 30
    
    def __init__(self, access_token: str):
        self.access_token = access_token
        self.headers = {
            'Accept': 'application/json',
            'Authorization': f'Bearer {access_token}'
        }
    
    async def needs_refresh(self, session: AsyncSession) -> bool:
        """
        Check if instruments need refresh
        
        Returns:
            True if needs refresh (> 30 days old or empty)
        """
        try:
            # Get latest instrument timestamp
            stmt = select(func.max(Instrument.created_at)).where(
                Instrument.exchange == 'NSE_FO'
            )
            result = await session.execute(stmt)
            last_update = result.scalar()
            
            if not last_update:
                logger.info("📋 No instruments found - refresh needed")
                return True
            
            days_old = (datetime.utcnow() - last_update).days
            
            if days_old >= self.REFRESH_DAYS:
                logger.info(f"📋 Instruments are {days_old} days old - refresh needed")
                return True
            
            logger.info(f"📋 Instruments are {days_old} days old - still fresh")
            return False
        
        except Exception as e:
            logger.error(f"❌ Error checking refresh: {e}")
            return True
    
    def fetch_all_option_contracts(
        self,
        instrument_key: str = "NSE_INDEX|Nifty 50"
    ) -> List[Dict]:
        """
        Fetch ALL option contracts for all expiries
        
        Args:
            instrument_key: Index key
            
        Returns:
            List of ALL contracts (all expiries)
        """
        url = f"{self.BASE_URL}/option/contract"
        
        params = {
            'instrument_key': instrument_key
            # No expiry_date = ALL expiries
        }
        
        logger.info(f"📥 Fetching ALL option contracts for {instrument_key}")
        logger.info("   This will take 20-30 seconds...")
        
        try:
            response = requests.get(url, headers=self.headers, params=params, timeout=60)
            response.raise_for_status()
            
            data = response.json()
            
            if data.get('status') == 'success':
                contracts = data.get('data', [])
                logger.info(f"✅ Fetched {len(contracts)} contracts (all expiries)")
                return contracts
            else:
                logger.error(f"❌ API error: {data}")
                return []
        
        except Exception as e:
            logger.error(f"❌ Error fetching: {e}")
            return []
    
    async def clear_old_instruments(self, session: AsyncSession):
        """Clear old Nifty instruments before sync"""
        try:
            stmt = delete(Instrument).where(
                Instrument.exchange == 'NSE_FO'
            )
            await session.execute(stmt)
            await session.commit()
            logger.info("🗑️  Cleared old instruments")
        except Exception as e:
            logger.error(f"❌ Error clearing: {e}")
    
    async def save_all_contracts(
        self,
        session: AsyncSession,
        contracts: List[Dict]
    ):
        """
        Save ALL contracts to database
        
        Args:
            session: AsyncSession
            contracts: All contracts
        """
        saved = 0
        
        for c in contracts:
            try:
                inst = Instrument(
                    instrument_key=c.get('instrument_key'),
                    exchange='NSE_FO',
                    symbol=c.get('tradingsymbol'),
                    strike=float(c.get('strike_price', 0)),
                    option_type=c.get('option_type'),
                    expiry=self._parse_date(c.get('expiry')),
                    lot_size=int(c.get('lot_size', 0)),
                    tick_size=0.05
                )
                session.add(inst)
                saved += 1
                
                if saved % 100 == 0:
                    logger.info(f"   Saving... {saved} contracts")
            
            except Exception as e:
                logger.warning(f"⚠️  Skip: {e}")
        
        await session.commit()
        logger.info(f"✅ Saved {saved} contracts to database")
    
    def _parse_date(self, date_str: str) -> Optional[datetime]:
        try:
            return datetime.strptime(date_str, '%Y-%m-%d')
        except:
            return None
    
    async def sync_instruments(
        self,
        instrument_key: str = "NSE_INDEX|Nifty 50",
        force: bool = False
    ):
        """
        Full sync workflow
        
        Args:
            instrument_key: Index key
            force: Force refresh even if fresh
        """
        logger.info("=" * 70)
        logger.info("Instrument Sync Service")
        logger.info("=" * 70)
        
        async for session in get_async_session():
            # Check if refresh needed
            if not force:
                needs = await self.needs_refresh(session)
                if not needs:
                    logger.info("✅ Instruments are fresh - no sync needed")
                    return
            
            logger.info("🔄 Starting full sync...")
            
            # Fetch ALL contracts
            contracts = self.fetch_all_option_contracts(instrument_key)
            
            if not contracts:
                logger.error("❌ No contracts fetched")
                return
            
            # Clear old data
            await self.clear_old_instruments(session)
            
            # Save all
            await self.save_all_contracts(session, contracts)
            
            logger.info("=" * 70)
            logger.info("✅ Sync complete!")
            logger.info("=" * 70)
            
            break


# ========================
# CLI
# ========================
if __name__ == "__main__":
    import asyncio
    import json
    
    async def main():
        print("=" * 70)
        print("Instrument Sync Service - Monthly Refresh")
        print("=" * 70)
        print()
        
        # Load token
        token_file = Path("data/upstox_token.json")
        
        if not token_file.exists():
            print("❌ No token!")
            return
        
        with open(token_file) as f:
            access_token = json.load(f).get('access_token')
        
        print(f"✅ Token: {access_token[:20]}...")
        print()
        
        # Create service
        service = InstrumentSyncService(access_token)
        
        # Option
        print("Options:")
        print("1. Check if refresh needed")
        print("2. Force sync (refresh all)")
        print()
        
        choice = input("Select (1-2): ").strip()
        
        if choice == "1":
            # Just check
            async for session in get_async_session():
                needs = await service.needs_refresh(session)
                if needs:
                    print("\n⚠️  Refresh recommended!")
                    sync = input("Sync now? (y/n): ").strip().lower()
                    if sync == 'y':
                        await service.sync_instruments()
                break
        
        elif choice == "2":
            # Force sync
            print("\n⚠️  This will fetch and store ALL Nifty option contracts")
            print("   This may take 30-60 seconds")
            confirm = input("Continue? (y/n): ").strip().lower()
            
            if confirm == 'y':
                await service.sync_instruments(force=True)
        
        print()
        print("=" * 70)
    
    asyncio.run(main())