"""
Base Event Class
All events inherit from this base class
"""

from datetime import datetime
from uuid import uuid4, UUID
from typing import Any, Dict
from pydantic import BaseModel, Field, ConfigDict

# Handle imports
try:
    from ..utils.timezone import now_ist
except ImportError:
    import sys
    from pathlib import Path
    project_root = Path(__file__).parent.parent.parent
    sys.path.insert(0, str(project_root))
    from src.utils.timezone import now_ist


class BaseEvent(BaseModel):
    """
    Base class for all events in the system
    
    All events have:
    - Unique event_id
    - Timestamp (in IST)
    - Event type identifier
    """
    
    model_config = ConfigDict(
        arbitrary_types_allowed=True,
        json_encoders={
            datetime: lambda v: v.isoformat(),
            UUID: lambda v: str(v),
        }
    )
    
    event_id: UUID = Field(
        default_factory=uuid4,
        description="Unique event identifier"
    )
    
    timestamp: datetime = Field(
        default_factory=now_ist,
        description="Event timestamp in IST"
    )
    
    event_type: str = Field(
        default="base.event",
        description="Event type identifier"
    )
    
    def to_dict(self) -> Dict[str, Any]:
        """
        Convert event to dictionary for JSON serialization
        
        Returns:
            Dictionary representation of event
        """
        return self.model_dump(mode='json')
    
    def to_json(self) -> str:
        """
        Convert event to JSON string
        
        Returns:
            JSON string representation
        """
        return self.model_dump_json()
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "BaseEvent":
        """
        Create event from dictionary
        
        Args:
            data: Dictionary with event data
            
        Returns:
            Event instance
        """
        return cls(**data)
    
    @classmethod
    def from_json(cls, json_str: str) -> "BaseEvent":
        """
        Create event from JSON string
        
        Args:
            json_str: JSON string
            
        Returns:
            Event instance
        """
        return cls.model_validate_json(json_str)
    
    def __repr__(self):
        return f"<{self.__class__.__name__} id={self.event_id} type={self.event_type}>"


# ========================
# Testing
# ========================
if __name__ == "__main__":
    """
    Test base event
    Run: uv run python src/events/base.py
    """
    import json
    
    print("=" * 60)
    print("Base Event Test")
    print("=" * 60)
    print()
    
    # Create event
    event = BaseEvent(event_type="test.event")
    
    print("1. Event Created:")
    print("-" * 60)
    print(f"   ID:        {event.event_id}")
    print(f"   Type:      {event.event_type}")
    print(f"   Timestamp: {event.timestamp}")
    print()
    
    # Convert to dict
    print("2. To Dictionary:")
    print("-" * 60)
    event_dict = event.to_dict()
    print(f"   {json.dumps(event_dict, indent=2, default=str)}")
    print()
    
    # Convert to JSON
    print("3. To JSON:")
    print("-" * 60)
    event_json = event.to_json()
    print(f"   {event_json}")
    print()
    
    # From JSON
    print("4. From JSON:")
    print("-" * 60)
    reconstructed = BaseEvent.from_json(event_json)
    print(f"   ID Match:        {reconstructed.event_id == event.event_id}")
    print(f"   Type Match:      {reconstructed.event_type == event.event_type}")
    print(f"   Timestamp Match: {reconstructed.timestamp == event.timestamp}")
    print()
    
    print("=" * 60)
    print("✅ Base event working!")
    print("=" * 60)