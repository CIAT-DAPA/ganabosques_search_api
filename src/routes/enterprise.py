import re
from fastapi import Query, HTTPException
from typing import Optional, List
from pydantic import BaseModel, Field
from bson import ObjectId
from ganabosques_orm.collections.enterprise import Enterprise
from tools.pagination import build_paginated_response, PaginatedResponse
from schemas.logschema import LogSchema

from routes.base_route import generate_read_only_router

from datetime import datetime
from ganabosques_orm.enums.typeenterprise import TypeEnterprise
from ganabosques_orm.enums.label import Label

class ExtIdEnterpriseSchema(BaseModel):
    label: Label = Field(..., description="Label type for the external ID")
    ext_code: str = Field(..., description="External code associated with the label")

class EnterpriseSchema(BaseModel):
    id: str = Field(..., description="Internal MongoDB ID of the enterprise")
    adm2_id: Optional[str] = Field(None, description="ID of the associated Adm2 document")
    name: Optional[str] = Field(None, description="Name of the enterprise")
    ext_id: List[ExtIdEnterpriseSchema] = Field(..., description="List of external identifiers")
    type_enterprise: TypeEnterprise = Field(..., description="Type of the enterprise (e.g., SLAUGHTERHOUSE, COLLECTION_CENTER)")
    latitude: Optional[float] = Field(None, description="Latitude of the enterprise location")
    longitud: Optional[float] = Field(None, description="Longitude of the enterprise location")
    log: Optional[LogSchema] = Field(None, description="Logging metadata")

    class Config:
        from_attributes = True
        json_schema_extra = {
            "example": {
                "id": "665f1726b1ac3457e3a91a02",
                "adm2_id": "664f2222b1ac3457e3a90001",
                "name": "Centro de Acopio El Roble",
                "ext_id": [
                    {
                        "label": "PRODUCTIONUNIT_ID",
                        "ext_code": "CA-2024-007"
                    }
                ],
                "type_enterprise": "COLLECTION_CENTER",
                "latitude": 3.4516,
                "longitud": -76.5320,
                "log": {
                    "enable": True,
                    "created": "2024-02-20T09:30:00Z",
                    "updated": "2025-06-10T11:45:00Z"
                }
            }
        }

def serialize_enterprise(doc):
    """Serialize an Enterprise document into a JSON-compatible dictionary."""
    return {
        "id": str(doc.id),
        "adm2_id": str(doc.adm2_id.id) if doc.adm2_id else None,
        "name": doc.name,
        "ext_id": [
            {
                "label": ext.label.value if ext.label else None,
                "ext_code": ext.ext_code
            } for ext in (doc.ext_id or [])
        ],
        "type_enterprise": doc.type_enterprise.value if doc.type_enterprise else None,
        "latitude": doc.latitude,
        "longitud": doc.longitud,
        "log": {
            "enable": doc.log.enable if doc.log else None,
            "created": doc.log.created.isoformat() if doc.log and doc.log.created else None,
            "updated": doc.log.updated.isoformat() if doc.log and doc.log.updated else None
        } if doc.log else None
    }

router = generate_read_only_router(
    prefix="/enterprise",
    tags=["Farm and Enterprise"],
    collection=Enterprise,
    schema_model=EnterpriseSchema,
    allowed_fields=["name", "type_enterprise"],
    serialize_fn=serialize_enterprise,
    include_endpoints=["paged","by-name"]
)

@router.get("/by-adm2", response_model=List[EnterpriseSchema])
def get_enterprise_by_adm2_ids(
    ids: str = Query(..., description="Comma-separated Adm2 IDs to filter Enterprises records")
):
    """
    Retrieve Enterprise records that belong to one or more Adm2 IDs.
    Example: /by-adm2?ids=665f1726b1ac3457e3a91a05,665f1726b1ac3457e3a91a06
    """
    search_ids = [id.strip() for id in ids.split(",") if id.strip()]
    invalid_ids = [i for i in search_ids if not ObjectId.is_valid(i)]
    if invalid_ids:
        raise HTTPException(
            status_code=400,
            detail=f"IDs no válidos: {', '.join(invalid_ids)}"
        )
    matches = Enterprise.objects(adm3_id__in=search_ids)
    return [serialize_enterprise(adm) for adm in matches]
