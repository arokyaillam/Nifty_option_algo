"""
Upstox Live Producer - Production Ready
Uses token from file + instruments from database
"""

import asyncio
import json
import ssl
import requests
from datetime import datetime
from typing import List, Optional
import logging

import sys
from pathlib import Path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from src.event_bus.bus import EventBus
from src.events.tick_events import TickReceivedEvent
from src.instruments.query_service import InstrumentQueryService
from src.database.engine import get_async_session
from src.config.settings import settings
from src.utils.timezone import now_ist

# Optional imports
try:
    import websockets
    WEBSOCKETS_AVAILABLE = True
except ImportError:
    WEBSOCKETS_AVAILABLE = False
    logging.warning("⚠️  websockets not installed")

try:
    from google.protobuf.json_format import MessageToDict
    PROTOBUF_AVAILABLE = True
except ImportError:
    PROTOBUF_AVAILABLE = False
    logging.warning("⚠️  protobuf not installed")

try:
    from src import MarketDataFeedV3_pb2 as pb
    PROTOBUF_FILE_AVAILABLE = True
except ImportError:
    pb = None
    PROTOBUF_FILE_AVAILABLE = False
    logging.warning("⚠️  MarketDataFeedV3_pb2.py not found")

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class UpstoxLiveProducer:
    """
    Production Upstox Producer
    
    Flow:
    1. Load token from data/upstox_token.json
    2. Get instrument keys from PostgreSQL
    3. Authorize WebSocket
    4. Subscribe to instruments
    5. Stream live ticks to event bus
    """
    
    BASE_URL = "https://api.upstox.com/v2"
    TOKEN_FILE = Path("data/upstox_token.json")
    
    def __init__(
        self,
        spot_price: float,
        expiry_date: str,  # YYYY-MM-DD
        event_bus: EventBus
    ):
        """
        Initialize producer
        
        Args:
            spot_price: Nifty spot price
            expiry_date: Expiry date (YYYY-MM-DD)
            event_bus: Event bus instance
        """
        self.spot_price = spot_price
        self.expiry_date = expiry_date
        self.event_bus = event_bus
        
        self.access_token: Optional[str] = None
        self.instrument_keys: List[str] = []
        self.websocket: Optional[websockets.WebSocketClientProtocol] = None
        
        self._running = False
        self.tick_count = 0
        
        # SSL context for Windows
        self.ssl_context = ssl.create_default_context()
        self.ssl_context.check_hostname = False
        self.ssl_context.verify_mode = ssl.CERT_NONE
    
    def load_token(self) -> bool:
        """
        Load access token from file
        
        Returns:
            True if loaded successfully
        """
        if not self.TOKEN_FILE.exists():
            logger.error(f"❌ Token file not found: {self.TOKEN_FILE}")
            logger.error("   Run: uv run python scripts/setup_upstox.py")
            return False
        
        try:
            with open(self.TOKEN_FILE) as f:
                token_data = json.load(f)
                self.access_token = token_data.get('access_token')
            
            if not self.access_token:
                logger.error("❌ No access token in file")
                return False
            
            logger.info(f"✅ Token loaded: {self.access_token[:20]}...")
            return True
        
        except Exception as e:
            logger.error(f"❌ Error loading token: {e}")
            return False
    
    async def load_instruments(self) -> bool:
        """
        Load instrument keys from database
        
        Returns:
            True if loaded successfully
        """
        try:
            async for session in get_async_session():
                service = InstrumentQueryService(session)
                
                self.instrument_keys = await service.get_instrument_keys(
                    self.spot_price,
                    self.expiry_date
                )
                
                if not self.instrument_keys:
                    logger.error("❌ No instruments found in database")
                    logger.error("   Run: uv run python src/instruments/sync_service.py")
                    return False
                
                logger.info(f"✅ Loaded {len(self.instrument_keys)} instrument keys")
                
                # Show first few
                for key in self.instrument_keys[:5]:
                    logger.info(f"   {key}")
                if len(self.instrument_keys) > 5:
                    logger.info(f"   ... and {len(self.instrument_keys) - 5} more")
                
                break
            
            return True
        
        except Exception as e:
            logger.error(f"❌ Error loading instruments: {e}")
            return False
    
    def get_websocket_auth(self) -> Optional[str]:
        """
        Get authorized WebSocket URI

        Returns:
            Authorized WebSocket URI
        """
        # WebSocket feed authorization uses v3 API
        url = "https://api.upstox.com/v3/feed/market-data-feed/authorize"
        
        headers = {
            'Accept': 'application/json',
            'Authorization': f'Bearer {self.access_token}'
        }
        
        logger.info("🔑 Getting WebSocket authorization...")
        
        try:
            response = requests.get(url, headers=headers, timeout=10)
            response.raise_for_status()
            
            data = response.json()
            
            if data.get('status') == 'success':
                ws_uri = data['data']['authorizedRedirectUri']
                logger.info("✅ WebSocket authorized")
                return ws_uri
            else:
                logger.error(f"❌ Authorization failed: {data}")
                return None
        
        except Exception as e:
            logger.error(f"❌ Authorization error: {e}")
            return None
    
    def decode_protobuf(self, buffer: bytes) -> Optional[dict]:
        """Decode protobuf message"""
        if not PROTOBUF_FILE_AVAILABLE or pb is None:
            logger.error("❌ Protobuf not available")
            return None

        try:
            feed_response = pb.FeedResponse()
            feed_response.ParseFromString(buffer)
            # Don't use preserving_proto_field_name - use default camelCase like Upstox example
            return MessageToDict(feed_response)

        except Exception as e:
            logger.error(f"❌ Protobuf decode error: {e}", exc_info=True)
            return None
    
    async def subscribe_instruments(self):
        """Send subscription message"""
        if not self.websocket:
            return

        subscription = {
            "guid": "trading_system",
            "method": "sub",
            "data": {
                "mode": "full",
                "instrumentKeys": self.instrument_keys
            }
        }

        logger.info(f"📡 Sending subscription for {len(self.instrument_keys)} instruments...")
        logger.info(f"🔍 Subscription mode: full")
        logger.info(f"🔍 First 3 instruments: {self.instrument_keys[:3]}")

        binary_data = json.dumps(subscription).encode('utf-8')
        await self.websocket.send(binary_data)

        logger.info(f"✅ Subscription message sent ({len(binary_data)} bytes)")
    
    async def handle_tick(self, message: bytes):
        """Handle incoming tick"""
        try:
            # Log first few messages for debugging
            if self.tick_count < 5:
                logger.info(f"🔍 Received binary message (size: {len(message)} bytes)")

            data_dict = self.decode_protobuf(message)

            if not data_dict:
                logger.warning("⚠️  Decoded protobuf is empty")
                return

            # Log decoded structure for first few messages
            if self.tick_count < 3:
                logger.info(f"🔍 Decoded data keys: {list(data_dict.keys())}")

            feeds = data_dict.get("feeds", {})

            if not feeds:
                logger.warning("⚠️  No 'feeds' in decoded data")
                if self.tick_count < 3:
                    logger.info(f"🔍 Full decoded data: {data_dict}")
                return

            # Log feed info for first message
            if self.tick_count == 0:
                logger.info(f"🔍 Number of instruments in feed: {len(feeds)}")

            for instrument_key, feed_info in feeds.items():
                # Log feed structure for first tick
                if self.tick_count == 0:
                    logger.info(f"🔍 Feed info keys for {instrument_key}: {list(feed_info.keys())}")
                    logger.info(f"🔍 Full feed_info structure: {feed_info}")

                # Upstox protobuf uses "ff" for full feed
                # Try both "ff" (preserved name) and possible camelCase variants
                full_feed = feed_info.get("ff") or feed_info.get("Ff") or feed_info.get("fullFeed")

                if not full_feed:
                    if self.tick_count < 3:
                        logger.warning(f"⚠️  No full feed data for {instrument_key}")
                        logger.info(f"🔍 Available feed keys: {list(feed_info.keys())}")
                        logger.info(f"🔍 Feed info dump: {feed_info}")
                    continue

                # Log full feed structure for first tick
                if self.tick_count == 0:
                    logger.info(f"🔍 Full feed data keys: {list(full_feed.keys()) if isinstance(full_feed, dict) else 'not a dict'}")

                # Create tick event - pass the full_feed as marketFF
                # The from_upstox_feed expects: {"fullFeed": {"marketFF": {...}}}
                # But Upstox sends: {"ff": {"marketFF": {...}}}
                # So we wrap it:
                tick_event = TickReceivedEvent.from_upstox_feed(
                    instrument_key=instrument_key,
                    feed_data={"fullFeed": full_feed}
                )

                # Publish
                await self.event_bus.publish(tick_event, "ticks")

                self.tick_count += 1

                # Log more frequently for visibility (every 10 ticks instead of 100)
                if self.tick_count % 10 == 0:
                    logger.info(
                        f"📊 Tick #{self.tick_count} | "
                        f"{instrument_key} @ ₹{tick_event.ltp} | "
                        f"Vol: {tick_event.volume} | OI: {tick_event.oi}"
                    )

        except Exception as e:
            logger.error(f"❌ Error handling tick: {e}", exc_info=True)
    
    async def start(self):
        """Start live producer"""
        logger.info("=" * 70)
        logger.info("🚀 Upstox Live Producer Starting")
        logger.info("=" * 70)
        
        # Check dependencies
        if not WEBSOCKETS_AVAILABLE:
            logger.error("❌ websockets not installed")
            logger.error("   Install: uv pip install websockets --break-system-packages")
            return
        
        if not PROTOBUF_AVAILABLE:
            logger.error("❌ protobuf not installed")
            logger.error("   Install: uv pip install protobuf --break-system-packages")
            return
        
        if not PROTOBUF_FILE_AVAILABLE:
            logger.error("❌ MarketDataFeedV3_pb2.py not found")
            logger.error("   Download from Upstox API docs")
            return
        
        # Load token
        if not self.load_token():
            return
        
        # Load instruments
        if not await self.load_instruments():
            return
        
        # Connect to event bus
        await self.event_bus.connect()
        
        logger.info(f"Configuration:")
        logger.info(f"  Spot: {self.spot_price}")
        logger.info(f"  Expiry: {self.expiry_date}")
        logger.info(f"  Instruments: {len(self.instrument_keys)}")
        logger.info("=" * 70)
        
        self._running = True
        
        while self._running:
            try:
                # Get WebSocket auth
                ws_uri = self.get_websocket_auth()
                
                if not ws_uri:
                    logger.error("❌ Cannot get WebSocket URI")
                    await asyncio.sleep(10)
                    continue
                
                # Connect
                logger.info("🔌 Connecting to WebSocket...")
                
                async with websockets.connect(ws_uri, ssl=self.ssl_context) as websocket:
                    self.websocket = websocket
                    logger.info("✅ Connected to Upstox WebSocket")
                    
                    # Wait a moment
                    await asyncio.sleep(1)
                    
                    # Subscribe
                    await self.subscribe_instruments()
                    
                    logger.info("🎧 Listening for ticks...")
                    logger.info("=" * 70)

                    message_count = 0

                    # Listen
                    while self._running:
                        message = await websocket.recv()
                        message_count += 1

                        # Log first few messages
                        if message_count <= 3:
                            msg_type = "BINARY" if isinstance(message, bytes) else "TEXT"
                            msg_size = len(message) if isinstance(message, bytes) else len(str(message))
                            logger.info(f"📦 Message #{message_count}: {msg_type} ({msg_size} bytes)")

                        if isinstance(message, bytes):
                            await self.handle_tick(message)
                        else:
                            # Text messages (subscription confirmations, errors, etc.)
                            logger.info(f"📝 Text message: {message}")
            
            except asyncio.CancelledError:
                logger.info("🛑 Producer stopped")
                break
            
            except Exception as e:
                logger.error(f"❌ Error: {e}")
                logger.info("⏳ Reconnecting in 5s...")
                await asyncio.sleep(5)
        
        # Cleanup
        await self.event_bus.disconnect()
        logger.info("✅ Producer shutdown complete")
    
    def stop(self):
        """Stop producer"""
        self._running = False


# ========================
# CLI
# ========================
if __name__ == "__main__":
    import signal
    
    async def main():
        print("=" * 70)
        print("Upstox Live Producer - Production Ready")
        print("=" * 70)
        print()
        
        # Get inputs
        spot = float(input("Enter Nifty spot price: "))
        expiry = input("Enter expiry date (YYYY-MM-DD): ").strip()
        
        print()
        
        # Create event bus
        bus = EventBus(redis_url=settings.get_redis_url)
        
        # Create producer
        producer = UpstoxLiveProducer(
            spot_price=spot,
            expiry_date=expiry,
            event_bus=bus
        )
        
        # Signal handler
        def signal_handler(sig, frame):
            producer.stop()
        
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
        
        # Start
        await producer.start()
    
    asyncio.run(main())