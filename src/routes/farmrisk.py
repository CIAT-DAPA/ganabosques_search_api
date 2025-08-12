import re
from fastapi import Query, HTTPException
from typing import Optional, List
from pydantic import BaseModel, Field
from bson import ObjectId
from ganabosques_orm.collections.farmrisk import FarmRisk
from tools.pagination import build_paginated_response, PaginatedResponse

from routes.base_route import generate_read_only_router
from tools.utils import parse_object_ids, build_search_query

class AttributeSchema(BaseModel):
    prop: Optional[float] = Field(None, description="Proportion")
    ha: Optional[float] = Field(None, description="Area in hectares")
    distance: Optional[float] = Field(None, description="Distance to feature")

class FarmRiskSchema(BaseModel):
    id: str = Field(..., description="MongoDB internal ID of the farm risk")
    farm_id: Optional[str] = Field(None, description="ID of the associated farm")
    farm_polygons_id: Optional[str] = Field(None, description="ID of the associated farm polygon")
    deforestation: Optional[AttributeSchema] = Field(None, description="Deforestation attributes")
    protected: Optional[AttributeSchema] = Field(None, description="Protected area attributes")
    risk_direct: Optional[float] = Field(None, description="Direct risk value")
    risk_input: Optional[float] = Field(None, description="Input-based risk value")
    risk_output: Optional[float] = Field(None, description="Output-based risk value")
    risk_total: Optional[float] = Field(None, description="Total risk value")

    class Config:
        from_attributes = True
        json_schema_extra = {
            "example": {
                "id": "6661bbbce2ac3457e3a92bbb",
                "farm_id": "665f1234b1ac3457e3a91aaa",
                "farm_polygons_id": "665f5678b1ac3457e3a91bbb",
                "deforestation": {
                    "prop": 0.25,
                    "ha": 12.5,
                    "distance": 1.3
                },
                "protected": {
                    "prop": 0.1,
                    "ha": 3.2,
                    "distance": 2.8
                },
                "risk_direct": 0.7,
                "risk_input": 0.6,
                "risk_output": 0.8,
                "risk_total": 2.1
            }
        }

def serialize_farmrisk(doc):
    """Serialize a FarmRisk document into a JSON-compatible dictionary."""
    return {
        "id": str(doc.id),
        "farm_id": str(doc.farm_id.id) if doc.farm_id else None,
        "farm_polygons_id": str(doc.farm_polygons_id.id) if doc.farm_polygons_id else None,
        "deforestation": {
            "prop": doc.deforestation.prop if doc.deforestation else None,
            "ha": doc.deforestation.ha if doc.deforestation else None,
            "distance": doc.deforestation.distance if doc.deforestation else None
        } if doc.deforestation else None,
        "protected": {
            "prop": doc.protected.prop if doc.protected else None,
            "ha": doc.protected.ha if doc.protected else None,
            "distance": doc.protected.distance if doc.protected else None
        } if doc.protected else None,
        "risk_direct": doc.risk_direct,
        "risk_input": doc.risk_input,
        "risk_output": doc.risk_output,
        "risk_total": doc.risk_total
    }

router = generate_read_only_router(
    prefix="/farmrisk",
    tags=["Analysis risk"],
    collection=FarmRisk,
    schema_model=FarmRiskSchema,
    allowed_fields=[],
    serialize_fn=serialize_farmrisk,
    include_endpoints=["paged"]
)
