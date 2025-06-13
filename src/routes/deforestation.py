import re
from fastapi import Query, HTTPException
from typing import Optional, List
from pydantic import BaseModel, Field
from bson import ObjectId
from ganabosques_orm.collections.deforestation import Deforestation
from tools.pagination import build_paginated_response, PaginatedResponse
from schemas.logschema import LogSchema

from routes.base_route import generate_read_only_router
from tools.utils import parse_object_ids, build_search_query

class DeforestationSchema(BaseModel):
    id: str = Field(..., description="MongoDB internal ID of the deforestation record")
    deforestation_source: str = Field(..., description="Source of the deforestation data (e.g., SMBYC)")
    deforestation_type: str = Field(..., description="Type of deforestation data: annual or cumulative")
    name: Optional[str] = Field(None, description="Name or label for the deforestation data")
    year_start: Optional[int] = Field(None, description="Start year of the deforestation period")
    year_end: Optional[int] = Field(None, description="End year of the deforestation period")
    path: Optional[str] = Field(None, description="Path or reference to the deforestation file in Geoserver")
    log: Optional[LogSchema] = Field(None, description="Logging information")

    class Config:
        from_attributes = True
        json_schema_extra = {
            "example": {
                "id": "665fdef0b1ac3457e3a91b01",
                "deforestation_source": "SMBYC",
                "deforestation_type": "annual",
                "name": "smbyc_deforestation_annual_2020_2023",
                "year_start": 2020,
                "year_end": 2023,
                "path": "deforestation/smbyc_deforestation_annual/",
                "log": {
                    "enable": True,
                    "created": "2023-02-01T12:00:00Z",
                    "updated": "2024-05-01T09:45:00Z"
                }
            }
        }

def serialize_deforestation(doc):
    """Serialize a Deforestation document into a JSON-compatible dictionary."""
    return {
        "id": str(doc.id),
        "deforestation_source": str(doc.deforestation_source.value) if doc.deforestation_source else None,
        "deforestation_type": str(doc.deforestation_type.value) if doc.deforestation_type else None,
        "name": doc.name,
        "year_start": doc.year_start,
        "year_end": doc.year_end,
        "path": doc.path,
        "log": {
            "enable": doc.log.enable if doc.log else None,
            "created": doc.log.created.isoformat() if doc.log and doc.log.created else None,
            "updated": doc.log.updated.isoformat() if doc.log and doc.log.updated else None
        } if doc.log else None
    }
router = generate_read_only_router(
    prefix="/deforestation",
    tags=["Spatial data"],
    collection=Deforestation,
    schema_model=DeforestationSchema,
    allowed_fields=["deforestation_source", "deforestation_type", "name", "year_start", "year_end"],
    serialize_fn=serialize_deforestation,
    include_endpoints=["paged", "by-name"]
)
