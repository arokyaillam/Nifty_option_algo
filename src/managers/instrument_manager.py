"""
Instrument Manager
Manage instrument subscriptions dynamically
"""

import asyncio
from typing import List, Set, Dict
from dataclasses import dataclass
from enum import Enum
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class FeedMode(str, Enum):
    """Feed modes"""
    LTPC = "ltpc"           # Last traded price + close
    FULL = "full"           # Full market depth (30 levels)
    FULL_D30 = "full_d30"   # Full with 30-depth (same as full)


@dataclass
class Subscription:
    """Subscription details"""
    instrument_key: str
    mode: FeedMode


class InstrumentManager:
    """
    Manage instrument subscriptions
    
    Features:
    - Add/remove instruments dynamically
    - Change feed mode
    - Track subscribed instruments
    - Send subscription updates to WebSocket
    """
    
    def __init__(self):
        """Initialize manager"""
        self.subscriptions: Dict[str, FeedMode] = {}
        self._websocket = None
        self._lock = asyncio.Lock()
    
    def set_websocket(self, websocket):
        """
        Set WebSocket connection for sending subscriptions
        
        Args:
            websocket: WebSocket connection
        """
        self._websocket = websocket
    
    def add_instrument(
        self,
        instrument_key: str,
        mode: FeedMode = FeedMode.FULL
    ):
        """
        Add instrument to subscriptions
        
        Args:
            instrument_key: Instrument to subscribe
            mode: Feed mode (ltpc/full/full_d30)
        """
        self.subscriptions[instrument_key] = mode
        logger.info(f"➕ Added: {instrument_key} (mode={mode})")
    
    def remove_instrument(self, instrument_key: str):
        """
        Remove instrument from subscriptions
        
        Args:
            instrument_key: Instrument to unsubscribe
        """
        if instrument_key in self.subscriptions:
            del self.subscriptions[instrument_key]
            logger.info(f"➖ Removed: {instrument_key}")
    
    def change_mode(self, instrument_key: str, mode: FeedMode):
        """
        Change feed mode for instrument
        
        Args:
            instrument_key: Instrument key
            mode: New feed mode
        """
        if instrument_key in self.subscriptions:
            self.subscriptions[instrument_key] = mode
            logger.info(f"🔄 Changed mode: {instrument_key} → {mode}")
    
    def get_subscribed_instruments(self) -> List[str]:
        """Get list of subscribed instruments"""
        return list(self.subscriptions.keys())
    
    def get_subscription_by_mode(self, mode: FeedMode) -> List[str]:
        """
        Get instruments subscribed with specific mode
        
        Args:
            mode: Feed mode
            
        Returns:
            List of instrument keys
        """
        return [
            key for key, m in self.subscriptions.items()
            if m == mode
        ]
    
    async def subscribe(self, instrument_keys: List[str], mode: FeedMode = FeedMode.FULL):
        """
        Subscribe to instruments via WebSocket
        
        Args:
            instrument_keys: List of instruments
            mode: Feed mode
        """
        async with self._lock:
            if not self._websocket:
                logger.error("❌ WebSocket not set")
                return
            
            # Add to local tracking
            for key in instrument_keys:
                self.add_instrument(key, mode)
            
            # Send subscription message
            message = {
                "guid": "someguid",
                "method": "sub",
                "data": {
                    "mode": mode.value,
                    "instrumentKeys": instrument_keys
                }
            }
            
            import json
            await self._websocket.send(json.dumps(message).encode('utf-8'))
            
            logger.info(f"📡 Subscribed to {len(instrument_keys)} instruments (mode={mode})")
    
    async def unsubscribe(self, instrument_keys: List[str]):
        """
        Unsubscribe from instruments
        
        Args:
            instrument_keys: List of instruments to unsubscribe
        """
        async with self._lock:
            if not self._websocket:
                logger.error("❌ WebSocket not set")
                return
            
            # Remove from local tracking
            for key in instrument_keys:
                self.remove_instrument(key)
            
            # Send unsubscribe message
            message = {
                "guid": "someguid",
                "method": "unsub",
                "data": {
                    "instrumentKeys": instrument_keys
                }
            }
            
            import json
            await self._websocket.send(json.dumps(message).encode('utf-8'))
            
            logger.info(f"📡 Unsubscribed from {len(instrument_keys)} instruments")
    
    async def change_mode_websocket(
        self,
        instrument_keys: List[str],
        new_mode: FeedMode
    ):
        """
        Change feed mode via WebSocket
        
        Args:
            instrument_keys: Instruments to change
            new_mode: New feed mode
        """
        async with self._lock:
            # Unsubscribe first
            await self.unsubscribe(instrument_keys)
            
            # Wait a moment
            await asyncio.sleep(0.1)
            
            # Subscribe with new mode
            await self.subscribe(instrument_keys, new_mode)
    
    def print_status(self):
        """Print subscription status"""
        print("=" * 70)
        print("Instrument Manager Status")
        print("=" * 70)
        print(f"Total subscriptions: {len(self.subscriptions)}")
        print()
        
        # Group by mode
        for mode in FeedMode:
            instruments = self.get_subscription_by_mode(mode)
            if instruments:
                print(f"{mode.value.upper()}:")
                for inst in instruments:
                    print(f"  • {inst}")
                print()
        
        print("=" * 70)


# ========================
# Testing
# ========================
if __name__ == "__main__":
    """
    Test instrument manager
    Run: uv run python src/managers/instrument_manager.py
    """
    
    print("=" * 70)
    print("Instrument Manager Test")
    print("=" * 70)
    print()
    
    # Create manager
    mgr = InstrumentManager()
    
    # Add instruments
    mgr.add_instrument("NSE_FO|61755", FeedMode.FULL)
    mgr.add_instrument("NSE_INDEX|Nifty 50", FeedMode.LTPC)
    mgr.add_instrument("NSE_INDEX|Nifty Bank", FeedMode.LTPC)
    
    # Print status
    mgr.print_status()
    
    # Change mode
    mgr.change_mode("NSE_FO|61755", FeedMode.LTPC)
    
    # Remove instrument
    mgr.remove_instrument("NSE_INDEX|Nifty Bank")
    
    # Final status
    mgr.print_status()
    
    print("✅ Instrument manager working!")