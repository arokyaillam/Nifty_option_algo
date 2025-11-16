"""
Application Settings using Pydantic Settings
Loads configuration from environment variables (.env file)
"""

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict
from typing import Literal


class Settings(BaseSettings):
    """
    Application configuration settings
    All values can be overridden via environment variables
    """
    
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore"
    )
    
    # ========================
    # Application Settings
    # ========================
    app_env: Literal["development", "staging", "production"] = Field(
        default="development",
        description="Application environment"
    )
    
    debug: bool = Field(
        default=True,
        description="Debug mode"
    )
    
    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"] = Field(
        default="INFO",
        description="Logging level"
    )
    
    # ========================
    # Database Configuration
    # ========================
    postgres_user: str = Field(
        default="trader",
        description="PostgreSQL username"
    )
    
    postgres_password: str = Field(
        default="password",
        description="PostgreSQL password"
    )
    
    postgres_db: str = Field(
        default="trading_db",
        description="PostgreSQL database name"
    )
    
    postgres_host: str = Field(
        default="localhost",
        description="PostgreSQL host"
    )
    
    postgres_port: int = Field(
        default=5432,
        description="PostgreSQL port"
    )
    
    database_url: str | None = Field(
        default=None,
        description="Complete database URL (overrides individual settings if provided)"
    )
    
    # Database connection pool settings
    db_pool_size: int = Field(
        default=5,
        description="Database connection pool size"
    )
    
    db_max_overflow: int = Field(
        default=10,
        description="Maximum overflow connections"
    )
    
    db_pool_timeout: int = Field(
        default=30,
        description="Pool timeout in seconds"
    )
    
    db_echo: bool = Field(
        default=False,
        description="Echo SQL statements (for debugging)"
    )
    
    @property
    def get_database_url(self) -> str:
        """
        Get complete database URL
        If DATABASE_URL is set, use it; otherwise construct from components
        """
        if self.database_url:
            return self.database_url
        
        return (
            f"postgresql+asyncpg://{self.postgres_user}:{self.postgres_password}"
            f"@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"
        )
    
    # ========================
    # Redis Configuration
    # ========================
    redis_host: str = Field(
        default="localhost",
        description="Redis host"
    )
    
    redis_port: int = Field(
        default=6379,
        description="Redis port"
    )
    
    redis_db: int = Field(
        default=0,
        description="Redis database number"
    )
    
    redis_url: str | None = Field(
        default=None,
        description="Complete Redis URL (overrides individual settings if provided)"
    )
    
    @property
    def get_redis_url(self) -> str:
        """
        Get complete Redis URL
        If REDIS_URL is set, use it; otherwise construct from components
        """
        if self.redis_url:
            return self.redis_url
        
        return f"redis://{self.redis_host}:{self.redis_port}/{self.redis_db}"
    
    # ========================
    # Upstox API Configuration
    # ========================
    upstox_api_key: str = Field(
        default="",
        description="Upstox API Key"
    )
    
    upstox_api_secret: str = Field(
        default="",
        description="Upstox API Secret"
    )
    
    upstox_access_token: str = Field(
        default="",
        description="Upstox Access Token"
    )
    
    upstox_redirect_uri: str = Field(
        default="http://localhost:8000/callback",
        description="Upstox OAuth redirect URI"
    )
    
    upstox_base_url: str = Field(
        default="https://api.upstox.com/v2",
        description="Upstox API base URL"
    )
    
    # ========================
    # Trading Configuration
    # ========================
    max_instruments: int = Field(
        default=1000,
        ge=1,
        le=10000,
        description="Maximum number of instruments to track"
    )
    
    tick_buffer_size: int = Field(
        default=300,
        ge=10,
        le=1000,
        description="Number of ticks to keep in buffer per instrument"
    )
    
    candle_interval_seconds: int = Field(
        default=60,
        ge=1,
        le=3600,
        description="Candle interval in seconds (default: 60 = 1 minute)"
    )
    
    # ========================
    # Market Configuration
    # ========================
    market_open_time: str = Field(
        default="09:15:00",
        description="Market opening time (IST) - HH:MM:SS format"
    )
    
    market_close_time: str = Field(
        default="15:30:00",
        description="Market closing time (IST) - HH:MM:SS format"
    )
    
    timezone: str = Field(
        default="Asia/Kolkata",
        description="Market timezone"
    )
    
    # ========================
    # Event Bus Configuration
    # ========================
    event_stream_max_len: int = Field(
        default=10000,
        ge=100,
        le=100000,
        description="Maximum events to keep in Redis stream"
    )
    
    event_consumer_batch_size: int = Field(
        default=10,
        ge=1,
        le=100,
        description="Number of events to read per batch"
    )
    
    event_consumer_block_ms: int = Field(
        default=1000,
        ge=100,
        le=10000,
        description="Consumer block timeout in milliseconds"
    )
    
    # ========================
    # Analysis Configuration
    # ========================
    panic_score_threshold: int = Field(
        default=60,
        ge=0,
        le=100,
        description="Minimum panic score to trigger buy signal"
    )
    
    gamma_spike_threshold: float = Field(
        default=0.5,
        ge=0.0,
        le=10.0,
        description="Gamma spike percentage threshold (0.5 = 50%)"
    )
    
    order_book_imbalance_threshold: float = Field(
        default=0.35,
        ge=0.0,
        le=1.0,
        description="Order book ratio threshold for panic detection"
    )
    
    # ========================
    # API Configuration
    # ========================
    api_host: str = Field(
        default="0.0.0.0",
        description="FastAPI host"
    )
    
    api_port: int = Field(
        default=8000,
        ge=1024,
        le=65535,
        description="FastAPI port"
    )
    
    api_reload: bool = Field(
        default=True,
        description="Auto-reload on code changes (development only)"
    )
    
    # ========================
    # Dashboard Configuration
    # ========================
    dash_host: str = Field(
        default="0.0.0.0",
        description="Dash dashboard host"
    )
    
    dash_port: int = Field(
        default=8050,
        ge=1024,
        le=65535,
        description="Dash dashboard port"
    )
    
    dash_debug: bool = Field(
        default=True,
        description="Dash debug mode"
    )
    
    # ========================
    # Validators
    # ========================
    @field_validator("market_open_time", "market_close_time")
    @classmethod
    def validate_time_format(cls, v: str) -> str:
        """Validate time format is HH:MM:SS"""
        from datetime import datetime
        try:
            datetime.strptime(v, "%H:%M:%S")
            return v
        except ValueError:
            raise ValueError(f"Time must be in HH:MM:SS format, got: {v}")
    
    # ========================
    # Helper Methods
    # ========================
    def is_production(self) -> bool:
        """Check if running in production"""
        return self.app_env == "production"
    
    def is_development(self) -> bool:
        """Check if running in development"""
        return self.app_env == "development"
    
    def get_log_config(self) -> dict:
        """Get logging configuration"""
        return {
            "version": 1,
            "disable_existing_loggers": False,
            "formatters": {
                "default": {
                    "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
                    "datefmt": "%Y-%m-%d %H:%M:%S",
                },
                "detailed": {
                    "format": "%(asctime)s - %(name)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s",
                    "datefmt": "%Y-%m-%d %H:%M:%S",
                },
            },
            "handlers": {
                "console": {
                    "class": "logging.StreamHandler",
                    "formatter": "default" if self.is_production() else "detailed",
                    "stream": "ext://sys.stdout",
                },
            },
            "root": {
                "level": self.log_level,
                "handlers": ["console"],
            },
        }


# ========================
# Global Settings Instance
# ========================
settings = Settings()


# ========================
# Convenience Functions
# ========================
def get_settings() -> Settings:
    """
    Get settings instance
    Use this in FastAPI dependencies
    """
    return settings


def reload_settings() -> Settings:
    """
    Reload settings from environment
    Useful for testing
    """
    global settings
    settings = Settings()
    return settings


# ========================
# Print Configuration (for debugging)
# ========================
if __name__ == "__main__":
    """
    Test settings loading
    Run: uv run python src/config/settings.py
    """
    import json
    from pydantic import SecretStr
    
    print("=" * 60)
    print("Application Settings")
    print("=" * 60)
    print()
    
    # Print all settings (mask sensitive data)
    config = settings.model_dump()
    
    # Mask sensitive fields
    sensitive_fields = [
        "postgres_password",
        "upstox_api_key", 
        "upstox_api_secret",
        "upstox_access_token"
    ]
    
    for field in sensitive_fields:
        if field in config and config[field]:
            config[field] = "***MASKED***"
    
    # Print nicely formatted
    print(json.dumps(config, indent=2, default=str))
    print()
    print("=" * 60)
    print("Derived URLs:")
    print("=" * 60)
    print(f"Database URL: {settings.get_database_url.split('@')[0]}@***MASKED***")
    print(f"Redis URL: {settings.get_redis_url}")
    print()
    print("=" * 60)
    print("✅ Settings loaded successfully!")
    print("=" * 60)