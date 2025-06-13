import re
from fastapi import Query, HTTPException
from typing import Optional, List
from pydantic import BaseModel, Field
from bson import ObjectId
from ganabosques_orm.collections.farm import Farm
from tools.pagination import build_paginated_response, PaginatedResponse
from schemas.logschema import LogSchema

from routes.base_route import generate_read_only_router
from tools.utils import parse_object_ids, build_search_query

from datetime import datetime
from ganabosques_orm.enums.source import Source
from ganabosques_orm.enums.farmsource import FarmSource

class ExtIdFarmSchema(BaseModel):
    source: Source = Field(..., description="Source system of the external ID")
    ext_code: str = Field(..., description="External code from the source")

class FarmSchema(BaseModel):
    id: str = Field(..., description="Internal MongoDB ID of the farm")
    adm3_id: str = Field(None, description="ID of the associated Adm3 document")
    ext_id: List[ExtIdFarmSchema] = Field(..., description="List of external identifiers")
    farm_source: FarmSource = Field(..., description="Source from which the farm was registered")
    log: Optional[LogSchema] = Field(None, description="Logging information")

    class Config:
        from_attributes = True
        json_schema_extra = {
            "example": {
                "id": "665f1726b1ac3457e3a91a01",
                "adm3_id": "664f1111b1ac3457e3a90000",
                "ext_id": [
                    {
                        "source": "SIT_CODE",
                        "ext_code": "SIT-2022-0001"
                    }
                ],
                "farm_source": "SAGARI",
                "log": {
                    "enable": True,
                    "created": "2024-01-15T14:20:00Z",
                    "updated": "2024-06-10T08:15:30Z"
                }
            }
        }

def serialize_farm(doc):
    """Serialize a Farm document into a JSON-compatible dictionary."""
    return {
        "id": str(doc.id),
        "adm3_id": str(doc.adm3_id.id) if doc.adm3_id else None,
        "ext_id": [
            {
                "source": str(ext.source.value),
                "ext_code": ext.ext_code
            } for ext in (doc.ext_id or [])
        ],
        "farm_source": str(doc.farm_source.value) if doc.farm_source else None,
        "log": {
            "enable": doc.log.enable if doc.log else None,
            "created": doc.log.created.isoformat() if doc.log and doc.log.created else None,
            "updated": doc.log.updated.isoformat() if doc.log and doc.log.updated else None
        } if doc.log else None
    }

router = generate_read_only_router(
    prefix="/farm",
    tags=["Farm and Enterprise"],
    collection=Farm,
    schema_model=FarmSchema,
    allowed_fields=["farm_source"],
    serialize_fn=serialize_farm,
    include_endpoints=["paged"]
)

@router.get("/by-adm3", response_model=List[FarmSchema])
def get_farm_by_adm3_ids(
    ids: str = Query(..., description="Comma-separated Adm3 IDs to filter Farms records")
):
    """
    Retrieve Farm records that belong to one or more Adm3 IDs.
    Example: /by-adm3?ids=665f1726b1ac3457e3a91a05,665f1726b1ac3457e3a91a06
    """
    search_ids = parse_object_ids(ids)
    matches = Farm.objects(adm3_id__in=search_ids)
    return [serialize_farm(adm) for adm in matches]
