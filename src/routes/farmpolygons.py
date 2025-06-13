import re
import json
from fastapi import Query, HTTPException
from typing import Optional, List, Dict, Any
from pydantic import BaseModel, Field
from bson import ObjectId
from ganabosques_orm.collections.farmpolygons import FarmPolygons
from tools.pagination import build_paginated_response, PaginatedResponse
from schemas.logschema import LogSchema

from routes.base_route import generate_read_only_router
from tools.utils import parse_object_ids, build_search_query

from ganabosques_orm.enums.ugg import UGG
from ganabosques_orm.enums.species import Species

class BufferPolygonSchema(BaseModel):
    ugg: Optional[UGG] = Field(None, description="UGG category of the animals")
    amount: Optional[int] = Field(None, description="Quantity of animals in this UGG category")
    species: Optional[Species] = Field(None, description="Species of the animals in this buffer")

class FarmPolygonsSchema(BaseModel):
    id: str = Field(..., description="Internal MongoDB ID of the FarmPolygons")
    farm_id: Optional[str] = Field(None, description="MongoDB ID reference to the Farm document")
    geojson: Optional[str] = Field(None, description="GeoJSON geometry string of the polygon")
    latitude: Optional[float] = Field(None, description="Latitude of the polygon centroid")
    longitud: Optional[float] = Field(None, description="Longitude of the polygon centroid")
    farm_ha: Optional[float] = Field(None, description="Hectares of the farm polygon")
    radio: Optional[float] = Field(None, description="Buffer radius used")
    buffer_inputs: Optional[List[BufferPolygonSchema]] = Field(None, description="List of buffer polygon objects")
    log: Optional[LogSchema] = Field(None, description="Logging information")

    class Config:
        from_attributes = True
        json_schema_extra = {
            "example": {
                "id": "665f1726b1ac3457e3a91a01",
                "farm_id": "665f1234b1ac3457e3a90000",
                "geojson": "{\"type\":\"Polygon\",...}",
                "latitude": 3.4516,
                "longitud": -76.5320,
                "farm_ha": 12.5,
                "radio": 500.0,
                "buffer_inputs": [
                    {
                        "ugg": "TERNEROS_MENORES_1_ANIO",
                        "amount": 20,
                        "species": "BOVINOS"
                    }
                ],
                "log": {
                    "enable": True,
                    "created": "2024-01-01T10:00:00Z",
                    "updated": "2024-06-01T08:30:00Z"
                }
            }
        }

def serialize_farm_polygon(doc):
    """Serialize a FarmPolygons document into a JSON-compatible dictionary."""
    return {
        "id": str(doc.id),
        "farm_id": str(doc.farm_id.id) if doc.farm_id else None,
        "geojson": doc.geojson,
        "latitude": doc.latitude,
        "longitud": doc.longitud,
        "farm_ha": doc.farm_ha,
        "radio": doc.radio,
        "buffer_inputs": [
            {
                "ugg": buffer.ugg.value if buffer.ugg else None,
                "amount": buffer.amount,
                "species": buffer.species.value if buffer.species else None
            } for buffer in (doc.buffer_inputs or [])
        ],
        "log": {
            "enable": doc.log.enable if doc.log else None,
            "created": doc.log.created.isoformat() if doc.log and doc.log.created else None,
            "updated": doc.log.updated.isoformat() if doc.log and doc.log.updated else None
        } if doc.log else None
    }

router = generate_read_only_router(
    prefix="/farmpolygons",
    tags=["Farm and Enterprise"],
    collection=FarmPolygons,
    schema_model=FarmPolygonsSchema,
    allowed_fields=[],
    serialize_fn=serialize_farm_polygon,
    include_endpoints=["paged"]
)

@router.get("/by-farm", response_model=List[FarmPolygonsSchema])
def get_farmpolygons_by_farm_ids(
    ids: str = Query(..., description="Comma-separated Farm IDs to filter FarmPolygonss records")
):
    """
    Retrieve FarmPolygons records that belong to one or more Farm IDs.
    Example: /by-farm?ids=665f1726b1ac3457e3a91a05,665f1726b1ac3457e3a91a06
    """
    search_ids = parse_object_ids(ids)
    matches = FarmPolygons.objects(farm_id__in=search_ids)
    return [serialize_farm_polygon(poly) for poly in matches]
