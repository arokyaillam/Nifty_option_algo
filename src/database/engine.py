"""
Database Engine Configuration
AsyncIO-based PostgreSQL connection with async session management
"""

from sqlalchemy.ext.asyncio import (
    create_async_engine,
    AsyncSession,
    async_sessionmaker
)
from sqlalchemy.pool import NullPool
from typing import AsyncGenerator
import logging

import sys
from pathlib import Path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from src.config.settings import settings

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# Create async engine
engine = create_async_engine(
    settings.get_database_url,
    echo=False,  # Set True for SQL query logging
    poolclass=NullPool,  # Disable connection pooling for async
    future=True
)

# Create async session factory
async_session_factory = async_sessionmaker(
    engine,
    class_=AsyncSession,
    expire_on_commit=False,
    autocommit=False,
    autoflush=False
)


async def get_async_session() -> AsyncGenerator[AsyncSession, None]:
    """
    Get async database session
    
    Usage:
        async for session in get_async_session():
            # Use session
            pass
    
    Yields:
        AsyncSession instance
    """
    async with async_session_factory() as session:
        try:
            yield session
        except Exception as e:
            await session.rollback()
            logger.error(f"❌ Session error: {e}")
            raise
        finally:
            await session.close()


async def test_connection():
    """
    Test database connection
    
    Returns:
        True if connection successful
    """
    try:
        async with engine.begin() as conn:
            from sqlalchemy import text
            result = await conn.execute(text("SELECT 1"))
            logger.info("✅ Database connection successful")
            return True
    except Exception as e:
        logger.error(f"❌ Database connection failed: {e}")
        return False


# ========================
# Testing
# ========================
if __name__ == "__main__":
    """
    Test database connection
    Run: uv run python src/database/engine.py
    """
    
    import asyncio
    
    async def test():
        print("=" * 70)
        print("Database Engine Test")
        print("=" * 70)
        print()
        
        print(f"Database URL: {settings.get_database_url}")
        print()
        
        # Test connection
        success = await test_connection()
        
        if success:
            print()
            print("Testing session creation...")
            
            async for session in get_async_session():
                print(f"✅ Session created: {session}")
                
                # Test query
                from sqlalchemy import text
                result = await session.execute(text("SELECT version()"))
                version = result.scalar()
                print(f"   PostgreSQL version: {version}")
                
                break
        
        print()
        print("=" * 70)
    
    asyncio.run(test())