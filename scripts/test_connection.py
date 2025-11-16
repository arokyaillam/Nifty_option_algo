"""
Test database and Redis connections
Run: uv run python scripts/test_connection.py
"""

import asyncio
import sys
from datetime import datetime


async def test_postgres():
    """Test PostgreSQL connection"""
    try:
        import asyncpg
        
        conn = await asyncpg.connect(
            host='localhost',
            port=5432,
            user='trader',
            password='password',
            database='trading_db'
        )
        
        # Test query
        version = await conn.fetchval('SELECT version()')
        print(f"✅ PostgreSQL Connected!")
        print(f"   Version: {version.split(',')[0]}")
        
        # Test create table (and drop)
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS test_table (
                id SERIAL PRIMARY KEY,
                name TEXT,
                created_at TIMESTAMPTZ DEFAULT NOW()
            )
        ''')
        
        await conn.execute('''
            INSERT INTO test_table (name) VALUES ($1)
        ''', 'test_connection')
        
        count = await conn.fetchval('SELECT COUNT(*) FROM test_table')
        print(f"   Test table: {count} rows")
        
        # Cleanup
        await conn.execute('DROP TABLE test_table')
        
        await conn.close()
        return True
        
    except Exception as e:
        print(f"❌ PostgreSQL Connection Failed!")
        print(f"   Error: {e}")
        return False


async def test_redis():
    """Test Redis connection"""
    try:
        import redis.asyncio as redis
        
        client = await redis.from_url('redis://localhost:6379')
        
        # Test ping
        pong = await client.ping()
        print(f"✅ Redis Connected!")
        print(f"   Ping: {pong}")
        
        # Test set/get
        test_key = f"test:{datetime.now().timestamp()}"
        await client.set(test_key, "Hello Redis!", ex=60)
        value = await client.get(test_key)
        print(f"   Set/Get test: {value.decode()}")
        
        # Test info
        info = await client.info('server')
        print(f"   Version: {info['redis_version']}")
        
        # Cleanup
        await client.delete(test_key)
        
        await client.aclose()
        return True
        
    except Exception as e:
        print(f"❌ Redis Connection Failed!")
        print(f"   Error: {e}")
        return False


async def main():
    """Run all connection tests"""
    print("=" * 50)
    print("Testing Database Connections")
    print("=" * 50)
    print()
    
    # Test PostgreSQL
    pg_ok = await test_postgres()
    print()
    
    # Test Redis
    redis_ok = await test_redis()
    print()
    
    # Summary
    print("=" * 50)
    if pg_ok and redis_ok:
        print("🎉 All connections successful!")
        print("=" * 50)
        sys.exit(0)
    else:
        print("⚠️  Some connections failed. Check logs above.")
        print("=" * 50)
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())