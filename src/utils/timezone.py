"""
Timezone Utilities for Indian Market Trading
All ticks come in UTC, process in IST, store in UTC

Critical for proper candle grouping and market hours validation
"""

from datetime import datetime, time as dt_time, timezone
from typing import Optional
import pytz

# ========================
# Timezone Constants
# ========================
IST = pytz.timezone('Asia/Kolkata')  # Indian Standard Time (UTC+5:30)
UTC = pytz.UTC


class TimezoneHandler:
    """Handle timezone conversions for trading system"""
    
    @staticmethod
    def utc_to_ist(dt: datetime) -> datetime:
        """
        Convert UTC datetime to IST
        
        Args:
            dt: UTC datetime (can be naive or aware)
            
        Returns:
            IST aware datetime
            
        Example:
            >>> utc_time = datetime(2024, 11, 16, 3, 45, 23, tzinfo=UTC)
            >>> ist_time = TimezoneHandler.utc_to_ist(utc_time)
            >>> print(ist_time)  # 2024-11-16 09:15:23+05:30
        """
        if dt.tzinfo is None:
            # Assume UTC if naive
            dt = UTC.localize(dt)
        elif dt.tzinfo != UTC:
            # Convert to UTC first if different timezone
            dt = dt.astimezone(UTC)
        
        return dt.astimezone(IST)
    
    @staticmethod
    def ist_to_utc(dt: datetime) -> datetime:
        """
        Convert IST datetime to UTC
        
        Args:
            dt: IST datetime (can be naive or aware)
            
        Returns:
            UTC aware datetime
            
        Example:
            >>> ist_time = datetime(2024, 11, 16, 9, 15, 23)
            >>> utc_time = TimezoneHandler.ist_to_utc(ist_time)
            >>> print(utc_time)  # 2024-11-16 03:45:23+00:00
        """
        if dt.tzinfo is None:
            # Localize to IST if naive
            dt = IST.localize(dt)
        elif dt.tzinfo != IST:
            # Convert to IST first if different timezone
            dt = dt.astimezone(IST)
        
        return dt.astimezone(UTC)
    
    @staticmethod
    def get_current_ist() -> datetime:
        """
        Get current time in IST
        
        Returns:
            Current IST datetime
        """
        return datetime.now(IST)
    
    @staticmethod
    def get_current_utc() -> datetime:
        """
        Get current time in UTC
        
        Returns:
            Current UTC datetime
        """
        return datetime.now(UTC)
    
    @staticmethod
    def parse_upstox_timestamp(timestamp_str: str) -> datetime:
        """
        Parse Upstox tick timestamp and convert to IST
        
        Upstox sends timestamps in format: "1747984841612" (milliseconds since epoch)
        OR ISO format: "2024-11-16T03:45:23.456Z"
        
        Args:
            timestamp_str: Timestamp from Upstox (milliseconds or ISO format)
            
        Returns:
            IST aware datetime
            
        Example:
            >>> ts = "1747984841612"
            >>> ist_time = TimezoneHandler.parse_upstox_timestamp(ts)
            >>> print(ist_time)  # IST datetime
        """
        try:
            # Try parsing as milliseconds since epoch (Upstox format)
            if timestamp_str.isdigit():
                timestamp_ms = int(timestamp_str)
                timestamp_sec = timestamp_ms / 1000.0
                dt = datetime.fromtimestamp(timestamp_sec, tz=UTC)
            else:
                # Parse as ISO format
                dt = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
            
            # Convert to IST
            return dt.astimezone(IST)
            
        except Exception as e:
            raise ValueError(f"Invalid timestamp format: {timestamp_str}") from e
    
    @staticmethod
    def get_candle_minute(dt: datetime) -> datetime:
        """
        Get candle boundary (minute start) in IST
        
        This is CRITICAL for proper candle grouping!
        
        Args:
            dt: Any datetime (will be converted to IST)
        
        Returns:
            IST datetime truncated to minute (seconds=0, microseconds=0)
        
        Example:
            Input:  2024-11-16 09:15:23.456 IST
            Output: 2024-11-16 09:15:00.000 IST
        """
        # Ensure IST
        if dt.tzinfo is None:
            dt = IST.localize(dt)
        else:
            dt = dt.astimezone(IST)
        
        # Truncate to minute
        return dt.replace(second=0, microsecond=0)
    
    @staticmethod
    def is_market_hours(dt: datetime, 
                       open_time: str = "09:15:00",
                       close_time: str = "15:30:00") -> bool:
        """
        Check if time is within market hours (IST)
        
        Args:
            dt: Datetime to check
            open_time: Market opening time (HH:MM:SS format, IST)
            close_time: Market closing time (HH:MM:SS format, IST)
            
        Returns:
            True if within market hours (09:15 - 15:30 IST by default)
            
        Example:
            >>> dt = datetime(2024, 11, 16, 10, 30, 0, tzinfo=IST)
            >>> is_open = TimezoneHandler.is_market_hours(dt)
            >>> print(is_open)  # True
        """
        # Convert to IST
        if dt.tzinfo is None:
            dt = IST.localize(dt)
        else:
            dt = dt.astimezone(IST)
        
        # Check time
        current_time = dt.time()
        
        # Parse market hours
        market_open = dt_time.fromisoformat(open_time)
        market_close = dt_time.fromisoformat(close_time)
        
        return market_open <= current_time <= market_close
    
    @staticmethod
    def is_weekday(dt: datetime) -> bool:
        """
        Check if date is a weekday (Monday-Friday)
        
        Args:
            dt: Datetime to check
            
        Returns:
            True if weekday (0=Monday to 4=Friday)
        """
        # Convert to IST
        if dt.tzinfo is None:
            dt = IST.localize(dt)
        else:
            dt = dt.astimezone(IST)
        
        return dt.weekday() < 5  # 0-4 = Mon-Fri, 5-6 = Sat-Sun
    
    @staticmethod
    def is_trading_day(dt: datetime,
                      open_time: str = "09:15:00",
                      close_time: str = "15:30:00") -> bool:
        """
        Check if it's a trading day and within market hours
        
        Args:
            dt: Datetime to check
            open_time: Market opening time
            close_time: Market closing time
            
        Returns:
            True if weekday AND within market hours
        """
        return (TimezoneHandler.is_weekday(dt) and 
                TimezoneHandler.is_market_hours(dt, open_time, close_time))
    
    @staticmethod
    def get_market_day(dt: datetime) -> datetime:
        """
        Get market day (date) in IST at 00:00:00
        
        Args:
            dt: Any datetime
            
        Returns:
            IST date at midnight (00:00:00)
        """
        # Convert to IST
        if dt.tzinfo is None:
            dt = IST.localize(dt)
        else:
            dt = dt.astimezone(IST)
        
        # Get date only (midnight IST)
        return dt.replace(hour=0, minute=0, second=0, microsecond=0)
    
    @staticmethod
    def format_ist(dt: datetime, format: str = "%Y-%m-%d %H:%M:%S %Z") -> str:
        """
        Format datetime in IST for display
        
        Args:
            dt: Datetime to format
            format: strftime format string
            
        Returns:
            Formatted string in IST
        """
        # Convert to IST
        if dt.tzinfo is None:
            dt = IST.localize(dt)
        else:
            dt = dt.astimezone(IST)
        
        return dt.strftime(format)
    
    @staticmethod
    def for_database(dt: datetime) -> datetime:
        """
        Prepare datetime for database storage (always UTC)
        
        Args:
            dt: Any datetime
            
        Returns:
            UTC aware datetime for database storage
        """
        if dt.tzinfo is None:
            # Assume IST if naive (since we work in IST)
            dt = IST.localize(dt)
        
        return dt.astimezone(UTC)


# ========================
# Convenience Functions
# ========================

def now_ist() -> datetime:
    """Get current IST time"""
    return TimezoneHandler.get_current_ist()


def now_utc() -> datetime:
    """Get current UTC time"""
    return TimezoneHandler.get_current_utc()


def parse_tick_timestamp(ts: str) -> datetime:
    """Parse Upstox timestamp to IST"""
    return TimezoneHandler.parse_upstox_timestamp(ts)


def candle_minute(dt: datetime) -> datetime:
    """Get candle boundary in IST"""
    return TimezoneHandler.get_candle_minute(dt)


def is_trading_time(dt: datetime) -> bool:
    """Check if market is open"""
    return TimezoneHandler.is_market_hours(dt)


def is_trading_day(dt: datetime) -> bool:
    """Check if it's a trading day"""
    return TimezoneHandler.is_trading_day(dt)


def to_ist(dt: datetime) -> datetime:
    """Convert to IST"""
    return TimezoneHandler.utc_to_ist(dt)


def to_utc(dt: datetime) -> datetime:
    """Convert to UTC"""
    return TimezoneHandler.ist_to_utc(dt)


# ========================
# Testing
# ========================
if __name__ == "__main__":
    """
    Test timezone utilities
    Run: uv run python src/utils/timezone.py
    """
    print("=" * 70)
    print("Timezone Utilities Test")
    print("=" * 70)
    print()
    
    # Test 1: Current times
    print("1. Current Times:")
    print("-" * 70)
    current_utc = now_utc()
    current_ist = now_ist()
    print(f"   UTC: {current_utc.strftime('%Y-%m-%d %H:%M:%S %Z')}")
    print(f"   IST: {current_ist.strftime('%Y-%m-%d %H:%M:%S %Z')}")
    print()
    
    # Test 2: Upstox timestamp parsing
    print("2. Upstox Timestamp Parsing:")
    print("-" * 70)
    upstox_ts = "1747984841612"  # Example from your data
    parsed = parse_tick_timestamp(upstox_ts)
    print(f"   Upstox timestamp: {upstox_ts}")
    print(f"   Parsed to IST:    {parsed.strftime('%Y-%m-%d %H:%M:%S.%f %Z')}")
    print()
    
    # Test 3: Candle minute calculation
    print("3. Candle Minute Boundary:")
    print("-" * 70)
    sample_time = datetime(2024, 11, 16, 9, 15, 23, 456789, tzinfo=IST)
    candle_time = candle_minute(sample_time)
    print(f"   Original: {sample_time.strftime('%Y-%m-%d %H:%M:%S.%f %Z')}")
    print(f"   Candle:   {candle_time.strftime('%Y-%m-%d %H:%M:%S.%f %Z')}")
    print()
    
    # Test 4: Market hours check
    print("4. Market Hours Validation:")
    print("-" * 70)
    
    test_times = [
        datetime(2024, 11, 16, 9, 0, 0, tzinfo=IST),   # Before open
        datetime(2024, 11, 16, 9, 15, 0, tzinfo=IST),  # Opening
        datetime(2024, 11, 16, 12, 0, 0, tzinfo=IST),  # Mid-day
        datetime(2024, 11, 16, 15, 30, 0, tzinfo=IST), # Closing
        datetime(2024, 11, 16, 16, 0, 0, tzinfo=IST),  # After close
    ]
    
    for test_time in test_times:
        is_open = is_trading_time(test_time)
        status = "✅ OPEN" if is_open else "❌ CLOSED"
        print(f"   {test_time.strftime('%H:%M:%S IST')} - {status}")
    print()
    
    # Test 5: UTC <-> IST conversion
    print("5. UTC <-> IST Conversion:")
    print("-" * 70)
    utc_sample = datetime(2024, 11, 16, 3, 45, 0, tzinfo=UTC)
    ist_converted = to_ist(utc_sample)
    utc_back = to_utc(ist_converted)
    
    print(f"   UTC Original:  {utc_sample.strftime('%Y-%m-%d %H:%M:%S %Z')}")
    print(f"   Converted IST: {ist_converted.strftime('%Y-%m-%d %H:%M:%S %Z')}")
    print(f"   Back to UTC:   {utc_back.strftime('%Y-%m-%d %H:%M:%S %Z')}")
    print(f"   Match: {'✅' if utc_sample == utc_back else '❌'}")
    print()
    
    # Test 6: Trading day check
    print("6. Trading Day Check:")
    print("-" * 70)
    
    # Monday
    monday = datetime(2024, 11, 18, 10, 0, 0, tzinfo=IST)
    # Saturday
    saturday = datetime(2024, 11, 16, 10, 0, 0, tzinfo=IST)
    
    print(f"   Monday 10:00 IST:   {'✅ Trading' if is_trading_day(monday) else '❌ Not trading'}")
    print(f"   Saturday 10:00 IST: {'✅ Trading' if is_trading_day(saturday) else '❌ Not trading'}")
    print()
    
    # Test 7: Database storage format
    print("7. Database Storage Format:")
    print("-" * 70)
    ist_now = now_ist()
    db_format = TimezoneHandler.for_database(ist_now)
    print(f"   IST Time:     {TimezoneHandler.format_ist(ist_now)}")
    print(f"   DB Format:    {db_format.strftime('%Y-%m-%d %H:%M:%S %Z')}")
    print()
    
    print("=" * 70)
    print("🎉 All timezone tests completed!")
    print("=" * 70)