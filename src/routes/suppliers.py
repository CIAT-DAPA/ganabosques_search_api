import re
import json
from fastapi import Query, HTTPException
from typing import Optional, List, Dict, Any
from pydantic import BaseModel, Field
from bson import ObjectId
from ganabosques_orm.collections.suppliers import Suppliers
from tools.pagination import build_paginated_response, PaginatedResponse
from schemas.logschema import LogSchema

from routes.base_route import generate_read_only_router
from tools.utils import parse_object_ids, build_search_query

class YearsSchema(BaseModel):
    years: str = Field(..., description="Year associated with the supplier relationship")

class SuppliersSchema(BaseModel):
    id: str = Field(..., description="MongoDB internal ID of the supplier record")
    enterprise_id: Optional[str] = Field(None, description="MongoDB ID of the associated enterprise")
    farm_id: Optional[str] = Field(None, description="MongoDB ID of the associated farm")
    years: Optional[List[YearsSchema]] = Field(None, description="List of years linked to this supplier relation")
    log: Optional[LogSchema] = Field(None, description="Logging information")

    class Config:
        from_attributes = True
        json_schema_extra = {
            "example": {
                "id": "665f1726b1ac3457e3a91a11",
                "enterprise_id": "665f1111b1ac3457e3a91a01",
                "farm_id": "665f2222b1ac3457e3a91a02",
                "years": [
                    {"years": "2022"},
                    {"years": "2023"}
                ],
                "log": {
                    "enable": True,
                    "created": "2023-04-15T10:30:00Z",
                    "updated": "2024-06-10T08:15:30Z"
                }
            }
        }

def serialize_supplier(doc):
    """Serialize a Suppliers document into a JSON-compatible dictionary."""
    return {
        "id": str(doc.id),
        "enterprise_id": str(doc.enterprise_id.id) if doc.enterprise_id else None,
        "farm_id": str(doc.farm_id.id) if doc.farm_id else None,
        "years": [{"years": y.years} for y in (doc.years or [])],
        "log": {
            "enable": doc.log.enable if doc.log else None,
            "created": doc.log.created.isoformat() if doc.log and doc.log.created else None,
            "updated": doc.log.updated.isoformat() if doc.log and doc.log.updated else None
        } if doc.log else None
    }

router = generate_read_only_router(
    prefix="/suppliers",
    tags=["Farm and Enterprise"],
    collection=Suppliers,
    schema_model=SuppliersSchema,
    allowed_fields=[],
    serialize_fn=serialize_supplier,
    include_endpoints=["paged"]
)

@router.get("/by-farm", response_model=List[SuppliersSchema])
def get_supplier_by_farm_ids(
    ids: str = Query(..., description="Comma-separated Farm IDs to filter Suppliers records")
):
    """
    Retrieve Suppliers records that belong to one or more Farm IDs.
    Example: /by-farm?ids=665f1726b1ac3457e3a91a05,665f1726b1ac3457e3a91a06
    """
    search_ids = parse_object_ids(ids)
    matches = Suppliers.objects(farm_id__in=search_ids)
    return [serialize_supplier(supe) for supe in matches]

@router.get("/by-enterprise", response_model=List[SuppliersSchema])
def get_supplier_by_farm_ids(
    ids: str = Query(..., description="Comma-separated Enterprise IDs to filter Suppliers records")
):
    """
    Retrieve Suppliers records that belong to one or more Enterprise IDs.
    Example: /by-enterprise?ids=665f1726b1ac3457e3a91a05,665f1726b1ac3457e3a91a06
    """
    search_ids = parse_object_ids(ids)
    matches = Suppliers.objects(enterprise_id__in=search_ids)
    return [serialize_supplier(supe) for supe in matches]
