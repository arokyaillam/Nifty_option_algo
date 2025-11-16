"""
Async SQLAlchemy Engine and Session Management
Provides database connection pooling and session factory
"""

from sqlalchemy.ext.asyncio import (
    create_async_engine,
    async_sessionmaker,
    AsyncSession,
    AsyncEngine
)
from sqlalchemy.pool import NullPool
from typing import AsyncGenerator
import logging

# Handle both relative and absolute imports
try:
    from ..config.settings import settings
except ImportError:
    # Running as standalone script
    import sys
    from pathlib import Path
    
    # Add project root to path
    project_root = Path(__file__).parent.parent.parent
    sys.path.insert(0, str(project_root))
    
    from src.config.settings import settings

logger = logging.getLogger(__name__)


# ========================
# Global Engine Instance
# ========================
engine: AsyncEngine | None = None
async_session_factory: async_sessionmaker[AsyncSession] | None = None


def create_engine() -> AsyncEngine:
    """
    Create async SQLAlchemy engine with connection pooling
    
    Returns:
        AsyncEngine configured for asyncpg
    """
    logger.info(f"Creating database engine: {settings.postgres_host}:{settings.postgres_port}")
    
    return create_async_engine(
        settings.get_database_url,
        echo=settings.db_echo,  # Log SQL statements if debug
        pool_size=settings.db_pool_size,  # Default: 5
        max_overflow=settings.db_max_overflow,  # Default: 10
        pool_timeout=settings.db_pool_timeout,  # Default: 30 seconds
        pool_pre_ping=True,  # Verify connections before using
        pool_recycle=3600,  # Recycle connections after 1 hour
    )


def create_session_factory() -> async_sessionmaker[AsyncSession]:
    """
    Create async session factory
    
    Returns:
        Session factory for creating database sessions
    """
    global engine
    
    if engine is None:
        engine = create_engine()
    
    return async_sessionmaker(
        bind=engine,
        class_=AsyncSession,
        expire_on_commit=False,  # Don't expire objects after commit
        autocommit=False,
        autoflush=False,
    )


def init_db() -> None:
    """
    Initialize database connection
    Call this at application startup
    """
    global engine, async_session_factory
    
    if engine is None:
        engine = create_engine()
        logger.info("✅ Database engine created")
    
    if async_session_factory is None:
        async_session_factory = create_session_factory()
        logger.info("✅ Session factory created")


async def close_db() -> None:
    """
    Close database connection
    Call this at application shutdown
    """
    global engine, async_session_factory
    
    if engine:
        await engine.dispose()
        logger.info("✅ Database engine disposed")
        engine = None
        async_session_factory = None


async def get_db() -> AsyncGenerator[AsyncSession, None]:
    """
    Dependency for FastAPI to get database session
    
    Usage:
        @app.get("/items")
        async def get_items(db: AsyncSession = Depends(get_db)):
            result = await db.execute(select(Item))
            return result.scalars().all()
    
    Yields:
        AsyncSession for database operations
    """
    if async_session_factory is None:
        init_db()
    
    async with async_session_factory() as session:
        try:
            yield session
            await session.commit()
        except Exception as e:
            await session.rollback()
            logger.error(f"Database session error: {e}")
            raise
        finally:
            await session.close()


async def get_db_session() -> AsyncSession:
    """
    Get a standalone database session (not for FastAPI dependency)
    Remember to close it manually!
    
    Usage:
        session = await get_db_session()
        try:
            result = await session.execute(select(Item))
            items = result.scalars().all()
        finally:
            await session.close()
    
    Returns:
        AsyncSession
    """
    if async_session_factory is None:
        init_db()
    
    return async_session_factory()


async def health_check() -> bool:
    """
    Check database connection health
    
    Returns:
        True if database is accessible, False otherwise
    """
    try:
        session = await get_db_session()
        try:
            # Simple query to test connection
            from sqlalchemy import text
            result = await session.execute(text("SELECT 1"))
            result.scalar()
            return True
        finally:
            await session.close()
    except Exception as e:
        logger.error(f"Database health check failed: {e}")
        return False


# ========================
# Context Manager for Sessions
# ========================
class DatabaseSession:
    """
    Context manager for database sessions
    
    Usage:
        async with DatabaseSession() as db:
            result = await db.execute(select(Item))
            items = result.scalars().all()
    """
    
    def __init__(self):
        self.session: AsyncSession | None = None
    
    async def __aenter__(self) -> AsyncSession:
        """Enter context - create session"""
        if async_session_factory is None:
            init_db()
        
        self.session = async_session_factory()
        return self.session
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Exit context - close session"""
        if self.session:
            if exc_type is not None:
                # Exception occurred, rollback
                await self.session.rollback()
            else:
                # Success, commit
                await self.session.commit()
            
            await self.session.close()


# ========================
# Testing
# ========================
if __name__ == "__main__":
    """
    Test database engine
    Run: uv run python src/database/engine.py
    Or: uv run python -m src.database.engine
    """
    import asyncio
    from sqlalchemy import text
    
    async def test_connection():
        print("=" * 60)
        print("Testing Database Engine")
        print("=" * 60)
        print()
        
        # Initialize
        init_db()
        print("✅ Engine initialized")
        print()
        
        # Health check
        is_healthy = await health_check()
        if is_healthy:
            print("✅ Database connection healthy")
        else:
            print("❌ Database connection failed")
            return
        print()
        
        # Test session
        print("Testing session creation...")
        async with DatabaseSession() as db:
            result = await db.execute(text("SELECT version()"))
            version = result.scalar()
            print(f"✅ PostgreSQL Version: {version.split(',')[0]}")
        print()
        
        # Cleanup
        await close_db()
        print("✅ Database engine closed")
        print()
        print("=" * 60)
        print("🎉 All tests passed!")
        print("=" * 60)
    
    asyncio.run(test_connection())