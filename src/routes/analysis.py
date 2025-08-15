import re
from fastapi import Query, HTTPException
from typing import Optional, List
from pydantic import BaseModel, Field
from bson import ObjectId
from ganabosques_orm.collections.analysis import Analysis
from tools.pagination import build_paginated_response, PaginatedResponse
from datetime import datetime

from routes.base_route import generate_read_only_router
from tools.utils import parse_object_ids, build_search_query


class AnalysisSchema(BaseModel):
    id: str = Field(..., description="MongoDB internal ID of the analysis")
    protected_areas_id: Optional[str] = Field(None, description="ID of the referenced ProtectedArea document")
    farming_areas_id: Optional[str] = Field(None, description="ID of the referenced FarmingArea document")
    deforestation_id: Optional[str] = Field(None, description="ID of the referenced Deforestation document")
    deforestation_source: str = Field(None, description="Source of the deforestation data (e.g., SMBYC)")
    deforestation_type: str = Field(None, description="Type of deforestation data: annual or cumulative")
    deforestation_name: Optional[str] = Field(None, description="Name or label for the deforestation data")
    deforestation_year_start: Optional[int] = Field(None, description="Start year of the deforestation period")
    deforestation_year_end: Optional[int] = Field(None, description="End year of the deforestation period")
    deforestation_path: Optional[str] = Field(None, description="Path or reference to the deforestation file in Geoserver")
    user_id: Optional[str] = Field(None, description="ID of the user who created the analysis")
    date: Optional[datetime] = Field(None, description="Datetime when the analysis was created")

    class Config:
        from_attributes = True
        json_schema_extra = {
            "example": {
                "id": "6660aaaab1ac3457e3a91f88",
                "protected_areas_id": "665faaaab1ac3457e3a91f01",
                "farming_areas_id": "665fbbbcb1ac3457e3a91f11",
                "deforestation_id": "665fcccdb1ac3457e3a91f22",
                "deforestation_source": "SMBYC",
                "deforestation_type": "annual",
                "deforestation_name": "smbyc_deforestation_annual_2020_2023",
                "deforestation_year_start": 2020,
                "deforestation_year_end": 2023,
                "deforestation_path": "deforestation/smbyc_deforestation_annual/",
                "user_id": "664f1234b1ac3457e3a90009",
                "date": "2025-06-11T14:30:00Z"
            }
        }

def serialize_analysis(doc):
    """Serialize an Analysis document into a JSON-compatible dictionary."""
    return {
        "id": str(doc.id),
        "protected_areas_id": str(doc.protected_areas_id.id) if doc.protected_areas_id else None,
        "farming_areas_id": str(doc.farming_areas_id.id) if doc.farming_areas_id else None,
        "deforestation_id": str(doc.deforestation_id.id) if doc.deforestation_id else None,
        "deforestation_source": str(doc.deforestation_id.deforestation_source.value) if doc.deforestation_id.deforestation_source else None,
        "deforestation_type": str(doc.deforestation_id.deforestation_type.value) if doc.deforestation_id.deforestation_type else None,
        "deforestation_name": str(doc.deforestation_id.name) if doc.deforestation_id.name else None,
        "deforestation_year_start": str(doc.deforestation_id.year_start) if doc.deforestation_id.year_start else None,
        "deforestation_year_end": str(doc.deforestation_id.year_end) if doc.deforestation_id.year_end else None,
        "deforestation_path": str(doc.deforestation_id.path) if doc.deforestation_id else None,
        "user_id": str(doc.user_id) if doc.user_id else None,
        "date": doc.date.isoformat() if doc.date else None
    }

router = generate_read_only_router(
    prefix="/analysis",
    tags=["Analysis risk"],
    collection=Analysis,
    schema_model=AnalysisSchema,
    allowed_fields=[],
    serialize_fn=serialize_analysis,
    include_endpoints=["paged"]
)
