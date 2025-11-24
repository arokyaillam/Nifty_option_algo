"""
Service Manager
Manage all system services (producers, consumers)
"""

import asyncio
import signal
from typing import Dict, Optional, List
from enum import Enum
from dataclasses import dataclass
from datetime import datetime
import logging

import sys
from pathlib import Path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from src.event_bus.bus import EventBus
from src.producers.upstox_live_producer import UpstoxLiveProducer
from src.consumers.candle_builder import CandleBuilder
from src.consumers.analysis_consumer import AnalysisConsumer
from src.consumers.storage_consumer import StorageConsumer
from src.config.settings import settings

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class ServiceStatus(str, Enum):
    """Service status"""
    STOPPED = "stopped"
    STARTING = "starting"
    RUNNING = "running"
    STOPPING = "stopping"
    ERROR = "error"


@dataclass
class ServiceInfo:
    """Service information"""
    name: str
    status: ServiceStatus
    task: Optional[asyncio.Task] = None
    started_at: Optional[datetime] = None
    error: Optional[str] = None


class ServiceManager:
    """
    Manage all system services

    Services:
    - Producer (Upstox live)
    - Candle Builder
    - Analysis Consumer
    - Storage Consumer
    """

    def __init__(self, spot_price: float, expiry_date: str):
        """
        Initialize service manager

        Args:
            spot_price: Nifty spot price
            expiry_date: Expiry date (YYYY-MM-DD)
        """
        self.spot_price = spot_price
        self.expiry_date = expiry_date
        self.services: Dict[str, ServiceInfo] = {}
        self.event_bus: Optional[EventBus] = None
        self._shutdown_event = asyncio.Event()
        self._running = False
    
    def _create_service_info(self, name: str) -> ServiceInfo:
        """Create service info"""
        info = ServiceInfo(
            name=name,
            status=ServiceStatus.STOPPED
        )
        self.services[name] = info
        return info
    
    async def start_producer(self):
        """Start Upstox live producer"""
        service_name = "upstox_producer"
        info = self._create_service_info(service_name)

        try:
            info.status = ServiceStatus.STARTING
            logger.info(f"🚀 Starting {service_name}...")

            producer = UpstoxLiveProducer(
                spot_price=self.spot_price,
                expiry_date=self.expiry_date,
                event_bus=self.event_bus
            )

            info.task = asyncio.create_task(producer.start())
            info.status = ServiceStatus.RUNNING
            info.started_at = datetime.now()

            logger.info(f"✅ {service_name} started")

            await info.task

        except asyncio.CancelledError:
            logger.info(f"🛑 {service_name} cancelled")
            info.status = ServiceStatus.STOPPED

        except Exception as e:
            logger.error(f"❌ {service_name} error: {e}", exc_info=True)
            info.status = ServiceStatus.ERROR
            info.error = str(e)
    
    async def start_candle_builder(self):
        """Start candle builder"""
        service_name = "candle_builder"
        info = self._create_service_info(service_name)
        
        try:
            info.status = ServiceStatus.STARTING
            logger.info(f"🚀 Starting {service_name}...")
            
            builder = CandleBuilder(event_bus=self.event_bus)
            
            info.task = asyncio.create_task(builder.start())
            info.status = ServiceStatus.RUNNING
            info.started_at = datetime.now()
            
            logger.info(f"✅ {service_name} started")
            
            await info.task
        
        except asyncio.CancelledError:
            logger.info(f"🛑 {service_name} cancelled")
            info.status = ServiceStatus.STOPPED
        
        except Exception as e:
            logger.error(f"❌ {service_name} error: {e}", exc_info=True)
            info.status = ServiceStatus.ERROR
            info.error = str(e)
    
    async def start_analysis_consumer(self):
        """Start analysis consumer"""
        service_name = "analysis_consumer"
        info = self._create_service_info(service_name)
        
        try:
            info.status = ServiceStatus.STARTING
            logger.info(f"🚀 Starting {service_name}...")
            
            consumer = AnalysisConsumer(event_bus=self.event_bus)
            
            info.task = asyncio.create_task(consumer.start())
            info.status = ServiceStatus.RUNNING
            info.started_at = datetime.now()
            
            logger.info(f"✅ {service_name} started")
            
            await info.task
        
        except asyncio.CancelledError:
            logger.info(f"🛑 {service_name} cancelled")
            info.status = ServiceStatus.STOPPED
        
        except Exception as e:
            logger.error(f"❌ {service_name} error: {e}", exc_info=True)
            info.status = ServiceStatus.ERROR
            info.error = str(e)
    
    async def start_storage_consumer(self):
        """Start storage consumer"""
        service_name = "storage_consumer"
        info = self._create_service_info(service_name)
        
        try:
            info.status = ServiceStatus.STARTING
            logger.info(f"🚀 Starting {service_name}...")
            
            consumer = StorageConsumer(event_bus=self.event_bus)
            
            info.task = asyncio.create_task(consumer.start())
            info.status = ServiceStatus.RUNNING
            info.started_at = datetime.now()
            
            logger.info(f"✅ {service_name} started")
            
            await info.task
        
        except asyncio.CancelledError:
            logger.info(f"🛑 {service_name} cancelled")
            info.status = ServiceStatus.STOPPED
        
        except Exception as e:
            logger.error(f"❌ {service_name} error: {e}", exc_info=True)
            info.status = ServiceStatus.ERROR
            info.error = str(e)
    
    async def start_all(self):
        """Start all services"""
        logger.info("=" * 70)
        logger.info("🚀 Starting Nifty Options Trading System")
        logger.info("=" * 70)
        
        # Create event bus
        self.event_bus = EventBus(redis_url=settings.get_redis_url)
        
        self._running = True
        
        # Start all services
        tasks = [
            asyncio.create_task(self.start_producer()),
            asyncio.create_task(self.start_candle_builder()),
            asyncio.create_task(self.start_analysis_consumer()),
            asyncio.create_task(self.start_storage_consumer())
        ]
        
        # Wait a moment for services to initialize
        await asyncio.sleep(2)
        
        logger.info("=" * 70)
        logger.info("✅ All services started successfully")
        logger.info("=" * 70)
        logger.info("")
        self.print_status()
        logger.info("")
        logger.info("Press Ctrl+C to stop all services")
        logger.info("=" * 70)
        
        # Wait for shutdown signal
        try:
            await self._shutdown_event.wait()
        except asyncio.CancelledError:
            pass
        
        # Stop all services
        await self.stop_all()
    
    async def stop_all(self):
        """Stop all services gracefully"""
        logger.info("")
        logger.info("=" * 70)
        logger.info("🛑 Stopping all services...")
        logger.info("=" * 70)
        
        # Cancel all tasks
        for name, info in self.services.items():
            if info.task and not info.task.done():
                logger.info(f"🛑 Stopping {name}...")
                info.status = ServiceStatus.STOPPING
                info.task.cancel()
        
        # Wait for all tasks to complete
        tasks = [info.task for info in self.services.values() if info.task]
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        
        # Close event bus
        if self.event_bus:
            await self.event_bus.disconnect()
        
        logger.info("=" * 70)
        logger.info("✅ All services stopped")
        logger.info("=" * 70)
    
    def print_status(self):
        """Print service status"""
        logger.info("Service Status:")
        logger.info("-" * 70)
        
        for name, info in self.services.items():
            status_icon = {
                ServiceStatus.STOPPED: "⚫",
                ServiceStatus.STARTING: "🟡",
                ServiceStatus.RUNNING: "🟢",
                ServiceStatus.STOPPING: "🟡",
                ServiceStatus.ERROR: "🔴"
            }.get(info.status, "⚪")
            
            uptime = ""
            if info.started_at and info.status == ServiceStatus.RUNNING:
                elapsed = (datetime.now() - info.started_at).total_seconds()
                uptime = f" (uptime: {int(elapsed)}s)"
            
            error_msg = f" - Error: {info.error}" if info.error else ""
            
            logger.info(f"{status_icon} {name:20s} {info.status:10s}{uptime}{error_msg}")
        
        logger.info("-" * 70)
    
    def signal_handler(self, sig, frame):
        """Handle shutdown signal"""
        logger.info("")
        logger.info("🛑 Shutdown signal received")
        self._shutdown_event.set()


# ========================
# Testing
# ========================
if __name__ == "__main__":
    """
    Test service manager
    Run: uv run python src/orchestrator/service_manager.py
    """
    
    async def test():
        print("=" * 70)
        print("Service Manager Test")
        print("=" * 70)
        print()

        # Get inputs
        spot = float(input("Enter Nifty spot price: "))
        expiry = input("Enter expiry date (YYYY-MM-DD): ").strip()

        print()

        manager = ServiceManager(
            spot_price=spot,
            expiry_date=expiry
        )

        # Setup signal handlers
        signal.signal(signal.SIGINT, manager.signal_handler)
        signal.signal(signal.SIGTERM, manager.signal_handler)

        await manager.start_all()

    asyncio.run(test())