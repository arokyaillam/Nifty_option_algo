"""
Upstox WebSocket Producer
Streams real-time market data from Upstox (Official implementation style)
"""

import asyncio
import json
import ssl
import logging
from typing import List, Optional
import requests

import sys
from pathlib import Path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from src.event_bus.bus import EventBus
from src.events.tick_events import TickReceivedEvent
from src.config.settings import settings
from src.utils.timezone import now_ist, is_trading_time

# Optional imports
try:
    import websockets
    WEBSOCKETS_AVAILABLE = True
except ImportError:
    WEBSOCKETS_AVAILABLE = False

try:
    from google.protobuf.json_format import MessageToDict
    PROTOBUF_AVAILABLE = True
except ImportError:
    PROTOBUF_AVAILABLE = False

# Import protobuf file (you'll need MarketDataFeedV3_pb2.py in project root or src/)
try:
    import MarketDataFeedV3_pb2 as pb
    PROTOBUF_FILE_AVAILABLE = True
except ImportError:
    try:
        # Try from src directory
        from src import MarketDataFeedV3_pb2 as pb
        PROTOBUF_FILE_AVAILABLE = True
    except ImportError:
        pb = None
        PROTOBUF_FILE_AVAILABLE = False

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class UpstoxProducer:
    """
    Stream ticks from Upstox WebSocket (Official implementation)
    
    Based on Upstox official sample code
    
    Requirements:
    1. websockets: uv pip install websockets --break-system-packages
    2. protobuf: uv pip install protobuf --break-system-packages
    3. requests: uv pip install requests --break-system-packages
    4. MarketDataFeedV3_pb2.py: Download from Upstox docs
    """
    
    def __init__(
        self,
        access_token: str,
        instrument_keys: List[str],
        event_bus: EventBus
    ):
        """
        Initialize Upstox producer
        
        Args:
            access_token: Upstox API access token
            instrument_keys: List of instruments to subscribe
            event_bus: Event bus instance
        """
        # Check dependencies
        if not WEBSOCKETS_AVAILABLE:
            raise ImportError("Install: uv pip install websockets --break-system-packages")
        
        if not PROTOBUF_AVAILABLE:
            raise ImportError("Install: uv pip install protobuf --break-system-packages")
        
        if not PROTOBUF_FILE_AVAILABLE:
            logger.warning(
                "⚠️  MarketDataFeedV3_pb2.py not found. "
                "Download from Upstox API docs and place in project root."
            )
        
        self.access_token = access_token
        self.instrument_keys = instrument_keys
        self.event_bus = event_bus
        
        self.websocket: Optional[websockets.WebSocketClientProtocol] = None
        self._running = False
        self._reconnect_delay = 5
        self.tick_count = 0
        
        # Create SSL context (needed for Windows)
        self.ssl_context = ssl.create_default_context()
        self.ssl_context.check_hostname = False
        self.ssl_context.verify_mode = ssl.CERT_NONE
    
    def get_market_data_feed_authorize(self) -> dict:
        """
        Get authorization for market data feed (Official Upstox API)
        
        Returns:
            Response with authorized WebSocket URI
        """
        headers = {
            'Accept': 'application/json',
            'Authorization': f'Bearer {self.access_token}'
        }
        
        url = 'https://api.upstox.com/v3/feed/market-data-feed/authorize'
        
        try:
            response = requests.get(url=url, headers=headers)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"❌ Authorization failed: {e}")
            raise
    
    def decode_protobuf(self, buffer: bytes) -> Optional[dict]:
        """
        Decode protobuf message (Official Upstox style)
        
        Args:
            buffer: Binary protobuf data
            
        Returns:
            Decoded message as dict
        """
        if not PROTOBUF_FILE_AVAILABLE or pb is None:
            logger.error("❌ MarketDataFeedV3_pb2.py not available")
            return None
        
        try:
            feed_response = pb.FeedResponse()
            feed_response.ParseFromString(buffer)
            return MessageToDict(feed_response, preserving_proto_field_name=True)
        except Exception as e:
            logger.error(f"❌ Protobuf decode error: {e}")
            return None
    
    async def _send_subscription(self):
        """Send subscription message"""
        if not self.websocket:
            return
        
        # Subscription data (Official Upstox format)
        data = {
            "guid": "someguid",
            "method": "sub",
            "data": {
                "mode": "full",
                "instrumentKeys": self.instrument_keys
            }
        }
        
        # Convert to binary and send
        binary_data = json.dumps(data).encode('utf-8')
        await self.websocket.send(binary_data)
        
        logger.info(f"📡 Subscribed to {len(self.instrument_keys)} instruments")
    
    async def _handle_message(self, message: bytes):
        """Handle incoming WebSocket message"""
        try:
            # Decode protobuf
            data_dict = self.decode_protobuf(message)
            
            if not data_dict:
                return
            
            # Extract feeds
            feeds = data_dict.get("feeds", {})
            
            for instrument_key, feed_info in feeds.items():
                # Get full feed
                full_feed = feed_info.get("ff", {})  # Note: Upstox uses 'ff' for full feed
                
                if not full_feed:
                    continue
                
                # Create tick event
                tick_event = TickReceivedEvent.from_upstox_feed(
                    instrument_key=instrument_key,
                    feed_data={"fullFeed": full_feed}  # Wrap in expected format
                )
                
                # Publish to event bus
                await self.event_bus.publish(tick_event, "ticks")
                
                self.tick_count += 1
                
                if self.tick_count % 100 == 0:
                    logger.info(
                        f"📊 Tick #{self.tick_count} | {instrument_key} @ {tick_event.ltp}"
                    )
        
        except Exception as e:
            logger.error(f"❌ Error handling message: {e}", exc_info=True)
    
    async def start(self):
        """Start streaming (Official Upstox style)"""
        await self.event_bus.connect()
        
        logger.info("🚀 Upstox producer starting...")
        logger.info(f"   Instruments: {len(self.instrument_keys)}")
        
        self._running = True
        
        while self._running:
            try:
                # Check market hours
                if not is_trading_time(now_ist()):
                    logger.info("⏸️  Outside market hours")
                    await asyncio.sleep(60)
                    continue
                
                # Get authorization
                logger.info("🔑 Getting WebSocket authorization...")
                auth_response = self.get_market_data_feed_authorize()
                ws_uri = auth_response["data"]["authorizedRedirectUri"]
                
                logger.info("🔌 Connecting to Upstox WebSocket...")
                
                # Connect with SSL context (Official style)
                async with websockets.connect(ws_uri, ssl=self.ssl_context) as websocket:
                    self.websocket = websocket
                    logger.info("✅ Connected to Upstox")
                    
                    # Wait a moment
                    await asyncio.sleep(1)
                    
                    # Subscribe
                    await self._send_subscription()
                    
                    # Listen for messages
                    while self._running:
                        message = await websocket.recv()
                        
                        # Handle binary message
                        if isinstance(message, bytes):
                            await self._handle_message(message)
                        else:
                            logger.debug(f"📝 Text: {message}")
            
            except asyncio.CancelledError:
                logger.info("🛑 Producer stopped")
                break
            
            except Exception as e:
                logger.error(f"❌ Error: {e}", exc_info=True)
                logger.info(f"⏳ Reconnecting in {self._reconnect_delay}s...")
                await asyncio.sleep(self._reconnect_delay)
        
        # Cleanup
        await self.event_bus.disconnect()
        logger.info("✅ Producer shutdown complete")
    
    def stop(self):
        """Stop producer"""
        self._running = False


# ========================
# Testing
# ========================
if __name__ == "__main__":
    print("=" * 70)
    print("Upstox Producer (Official Implementation)")
    print("=" * 70)
    print()
    
    print("Requirements:")
    print(f"  websockets:  {'✅' if WEBSOCKETS_AVAILABLE else '❌'}")
    print(f"  protobuf:    {'✅' if PROTOBUF_AVAILABLE else '❌'}")
    print(f"  pb2 file:    {'✅' if PROTOBUF_FILE_AVAILABLE else '❌'}")
    print()
    
    if not PROTOBUF_FILE_AVAILABLE:
        print("📥 Download MarketDataFeedV3_pb2.py:")
        print("   https://upstox.com/developer/api-documentation/market-data-feed")
        print("   Place in: D:\\Nifty_option_algo\\src\\")
        print()
    
    print("=" * 70)