"""
Run Complete Trading System
Single command to start everything
"""

import asyncio
import sys
from pathlib import Path

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.orchestrator.main import MainOrchestrator

if __name__ == "__main__":
    print("Starting Nifty Options Trading System...")
    print("Press Ctrl+C to stop")
    print()
    
    orchestrator = MainOrchestrator(enable_health_monitor=True)
    asyncio.run(orchestrator.start())