from typing import Optional
from pydantic import BaseModel, Field, ConfigDict
from ganabosques_orm.enums.source import Source
from ganabosques_orm.enums.label import Label

class ExtIdFarmSchema(BaseModel):
    """Optimized schema for Farm external ID - read-only."""
    source: Source = Field(..., description="Source system of the external ID")
    ext_code: Optional[str] = Field(None, description="External code from the source (can be null)")
    
    model_config = ConfigDict(from_attributes=True, use_enum_values=True)

class ExtIdEnterpriseSchema(BaseModel):
    """Optimized schema for Enterprise external ID - read-only."""
    label: Label = Field(..., description="Label type for the external ID")
    ext_code: Optional[str] = Field(None, description="External code associated with the label (can be null)")
    
    model_config = ConfigDict(from_attributes=True, use_enum_values=True)
