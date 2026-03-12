from datetime import datetime
from typing import Optional
from pydantic import BaseModel, Field, ConfigDict

class LogSchema(BaseModel):
    """Optimized schema for log data - read-only."""
    enable: Optional[bool] = Field(None, description="Whether the record is enabled")
    created: Optional[datetime] = Field(None, description="Creation timestamp")
    updated: Optional[datetime] = Field(None, description="Last update timestamp")
    
    model_config = ConfigDict(from_attributes=True)