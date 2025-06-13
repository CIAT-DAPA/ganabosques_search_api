from datetime import datetime
from pydantic import BaseModel, Field

class LogSchema(BaseModel):
    enable: bool = Field(None, description="Whether the record is enabled")
    created: datetime = Field(None, description="Creation timestamp")
    updated: datetime = Field(None, description="Last update timestamp")