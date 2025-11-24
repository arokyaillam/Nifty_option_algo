"""
Main Orchestrator
Single entry point to run entire trading system
"""

import asyncio
import signal
from datetime import datetime
import logging

import sys
from pathlib import Path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from src.orchestrator.service_manager import ServiceManager
from src.orchestrator.health_monitor import HealthMonitor
from src.config.settings import settings

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class MainOrchestrator:
    """
    Main system orchestrator

    Manages:
    - All services (producer, consumers)
    - Health monitoring
    - Graceful shutdown
    """

    def __init__(
        self,
        spot_price: float,
        expiry_date: str,
        enable_health_monitor: bool = True
    ):
        """
        Initialize orchestrator

        Args:
            spot_price: Nifty spot price
            expiry_date: Expiry date (YYYY-MM-DD)
            enable_health_monitor: Enable health monitoring
        """
        self.spot_price = spot_price
        self.expiry_date = expiry_date
        self.service_manager = ServiceManager(
            spot_price=spot_price,
            expiry_date=expiry_date
        )
        self.health_monitor = HealthMonitor(check_interval=60) if enable_health_monitor else None
        self._shutdown_event = asyncio.Event()
    
    def signal_handler(self, sig, frame):
        """Handle shutdown signal"""
        self._shutdown_event.set()
    
    async def start(self):
        """Start entire system"""
        # Print banner
        self.print_banner()
        
        # Setup signal handlers
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)
        
        # Start services
        service_task = asyncio.create_task(self.service_manager.start_all())
        
        # Start health monitor
        health_task = None
        if self.health_monitor:
            await asyncio.sleep(5)  # Wait for services to stabilize
            health_task = asyncio.create_task(self.health_monitor.start())
        
        # Wait for shutdown
        await self._shutdown_event.wait()
        
        # Stop health monitor
        if self.health_monitor:
            self.health_monitor.stop()
            if health_task:
                health_task.cancel()
        
        # Trigger service manager shutdown
        self.service_manager.signal_handler(None, None)
        
        # Wait for services to stop
        try:
            await service_task
        except asyncio.CancelledError:
            pass
    
    def print_banner(self):
        """Print startup banner"""
        banner = rf"""
{"=" * 70}
   _   _ _  __ _         ___        _   _
  | \ | (_)/ _| |_ _   _/ _ \ _ __ | |_(_) ___  _ __  ___
  |  \| | | |_| __| | | | | | | '_ \| __| |/ _ \| '_ \/ __|
  | |\  | |  _| |_| |_| | |_| | |_) | |_| | (_) | | | \__ \
  |_| \_|_|_|  \__|\__, |\___/| .__/ \__|_|\___/|_| |_|___/
                   |___/      |_|

         Algorithmic Trading System - Seller Panic Detection
{"=" * 70}

System Information:
  • Started: {datetime.now().strftime("%Y-%m-%d %H:%M:%S IST")}
  • Environment: {settings.app_env}
  • Redis: {settings.get_redis_url}
  • Database: {settings.postgres_db}

Trading Configuration:
  • Spot Price: {self.spot_price}
  • Expiry Date: {self.expiry_date}
  • Mode: LIVE (Upstox WebSocket)

Features:
  ✅ Real-time tick processing (Upstox WebSocket)
  ✅ 1-minute candle aggregation
  ✅ Seller panic detection
  ✅ BUY/SELL signal generation
  ✅ PostgreSQL persistence
  ✅ Health monitoring

Strategy Focus:
  🎯 Detect seller panic moments
  🎯 Identify short covering
  🎯 Capture gamma spikes
  🎯 Monitor order book imbalances

{"=" * 70}
"""
        print(banner)


# ========================
# Entry Point
# ========================
if __name__ == "__main__":
    """
    Main entry point
    Run: uv run python src/orchestrator/main.py
    """
    
    async def main():
        print("=" * 70)
        print("Nifty Options Trading System - Live Mode")
        print("=" * 70)
        print()

        # Get inputs
        spot = float(input("Enter Nifty spot price: "))
        expiry = input("Enter expiry date (YYYY-MM-DD): ").strip()

        print()

        orchestrator = MainOrchestrator(
            spot_price=spot,
            expiry_date=expiry,
            enable_health_monitor=True
        )
        await orchestrator.start()

    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Shutdown complete")