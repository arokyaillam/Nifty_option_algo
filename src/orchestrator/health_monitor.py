"""
Health Monitor
Monitor system health and performance
"""

import asyncio
from datetime import datetime
from typing import Dict
import logging

import sys
from pathlib import Path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from src.event_bus.bus import EventBus
from src.database.engine import get_async_session
from src.database.service import DatabaseService
from src.config.settings import settings

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class HealthMonitor:
    """
    Monitor system health
    
    Checks:
    - Redis connectivity
    - PostgreSQL connectivity
    - Stream sizes
    - Database row counts
    - Service responsiveness
    """
    
    def __init__(self, check_interval: int = 60):
        """
        Initialize health monitor
        
        Args:
            check_interval: Seconds between health checks
        """
        self.check_interval = check_interval
        self._running = False
    
    async def check_redis(self) -> Dict:
        """Check Redis connectivity and streams"""
        try:
            bus = EventBus(redis_url=settings.get_redis_url)
            await bus.connect()
            
            # Get stream lengths
            ticks_len = await bus.redis.xlen("ticks")
            candles_len = await bus.redis.xlen("candles")
            signals_len = await bus.redis.xlen("signals")
            
            await bus.disconnect()
            
            return {
                "status": "healthy",
                "ticks_count": ticks_len,
                "candles_count": candles_len,
                "signals_count": signals_len
            }
        
        except Exception as e:
            return {
                "status": "unhealthy",
                "error": str(e)
            }
    
    async def check_postgres(self) -> Dict:
        """Check PostgreSQL connectivity and data"""
        try:
            async for session in get_async_session():
                service = DatabaseService(session)
                
                candle_count = await service.get_candle_count()
                signal_count = await service.get_signal_count()
                
                return {
                    "status": "healthy",
                    "candle_count": candle_count,
                    "signal_count": signal_count
                }
        
        except Exception as e:
            return {
                "status": "unhealthy",
                "error": str(e)
            }
    
    async def run_health_check(self):
        """Run complete health check"""
        logger.info("=" * 70)
        logger.info("🏥 Health Check - " + datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        logger.info("=" * 70)
        
        # Check Redis
        redis_health = await self.check_redis()
        logger.info(f"Redis: {redis_health['status']}")
        if redis_health['status'] == 'healthy':
            logger.info(f"  Ticks stream: {redis_health['ticks_count']:,}")
            logger.info(f"  Candles stream: {redis_health['candles_count']:,}")
            logger.info(f"  Signals stream: {redis_health['signals_count']:,}")
        else:
            logger.error(f"  Error: {redis_health.get('error')}")
        
        # Check PostgreSQL
        postgres_health = await self.check_postgres()
        logger.info(f"PostgreSQL: {postgres_health['status']}")
        if postgres_health['status'] == 'healthy':
            logger.info(f"  Candles saved: {postgres_health['candle_count']:,}")
            logger.info(f"  Signals saved: {postgres_health['signal_count']:,}")
        else:
            logger.error(f"  Error: {postgres_health.get('error')}")
        
        logger.info("=" * 70)
    
    async def start(self):
        """Start health monitoring"""
        self._running = True
        
        logger.info(f"🏥 Health monitor started (interval: {self.check_interval}s)")
        
        while self._running:
            await self.run_health_check()
            await asyncio.sleep(self.check_interval)
    
    def stop(self):
        """Stop health monitoring"""
        self._running = False


if __name__ == "__main__":
    async def test():
        monitor = HealthMonitor(check_interval=30)
        await monitor.start()
    
    asyncio.run(test())