"""
Reset Database
Drop all tables and recreate with new schema
"""

import asyncio
import sys
from pathlib import Path

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.database.engine import engine
from src.database.models import Base
from sqlalchemy import text
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def reset_database():
    """Drop and recreate all tables"""
    
    print("=" * 70)
    print("Database Reset")
    print("=" * 70)
    print()
    
    print("⚠️  WARNING: This will DROP ALL TABLES!")
    print()
    
    confirm = input("Type 'YES' to confirm: ").strip()
    
    if confirm != "YES":
        print("❌ Cancelled")
        return
    
    print()
    print("Dropping all tables...")

    async with engine.begin() as conn:
        # Drop all tables with CASCADE to handle foreign key constraints
        await conn.execute(text("DROP SCHEMA public CASCADE"))
        await conn.execute(text("CREATE SCHEMA public"))
        await conn.execute(text("GRANT ALL ON SCHEMA public TO PUBLIC"))
        logger.info("✅ All tables dropped")
    
    print()
    print("Creating tables with new schema...")
    
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
        logger.info("✅ All tables created")
    
    print()
    print("=" * 70)
    print("✅ Database reset complete!")
    print("=" * 70)


if __name__ == "__main__":
    asyncio.run(reset_database())