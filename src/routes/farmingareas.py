import re
from fastapi import Query, HTTPException
from typing import Optional, List
from pydantic import BaseModel, Field
from bson import ObjectId
from ganabosques_orm.collections.farmingareas import FarmingAreas
from tools.pagination import build_paginated_response, PaginatedResponse
from schemas.logschema import LogSchema

from routes.base_route import generate_read_only_router
from tools.utils import parse_object_ids, build_search_query

class FarmingAreasSchema(BaseModel):
    id: str = Field(..., description="MongoDB internal ID of the protected area")
    name: Optional[str] = Field(None, description="Name of the protected area")
    path: Optional[str] = Field(None, description="Path to the geo file or resource")
    log: Optional[LogSchema] = Field(None, description="Logging metadata")

    class Config:
        from_attributes = True
        json_schema_extra = {
            "example": {
                "id": "665faaaab1ac3457e3a91f01",
                "name": "Parque Nacional Natural Farallones",
                "path": "/geo/protected/farallones.geojson",
                "log": {
                    "enable": True,
                    "created": "2022-08-01T12:00:00Z",
                    "updated": "2024-06-10T15:30:00Z"
                }
            }
        }

def serialize_farming_areas(doc):
    """Serialize a ProtectedAreas document into a JSON-compatible dictionary."""
    return {
        "id": str(doc.id),
        "name": doc.name,
        "path": doc.path,
        "log": {
            "enable": doc.log.enable if doc.log else None,
            "created": doc.log.created.isoformat() if doc.log and doc.log.created else None,
            "updated": doc.log.updated.isoformat() if doc.log and doc.log.updated else None
        } if doc.log else None
    }

router = generate_read_only_router(
    prefix="/farmingareas",
    tags=["Spatial data"],
    collection=FarmingAreas,
    schema_model=FarmingAreasSchema,
    allowed_fields=["name"],
    serialize_fn=serialize_farming_areas,
    include_endpoints=["paged", "by-name"]
)
