import re
from fastapi import APIRouter, Query, HTTPException
from typing import Optional, List
from pydantic import BaseModel, Field
from bson import ObjectId
from ganabosques_orm.collections.adm2 import Adm2
from tools.pagination import build_paginated_response, PaginatedResponse
from tools.utils import parse_object_ids, build_search_query

router = APIRouter(
    prefix="/adm2",
    tags=["Admin levels"]
)

class Adm2Schema(BaseModel):
    id: str = Field(..., description="Internal MongoDB ID")
    ext_id: Optional[str] = Field(None, description="External administrative region ID")
    name: Optional[str] = Field(None, description="Name of the administrative region")
    adm1_id: Optional[str] = Field(None, description="ID of the adm1 level to which it belongs")
    adm1_name: Optional[str] = Field(None, description="Name of the level adm1 to which it belongs")

    class Config:
        from_attributes = True
        json_schema_extra = {
            "example": {
                "id": "66602cd389f18226a0d9a2aa",
                "ext_id": "5001",
                "name": "MEDELLÍN",
                "adm1_id": "665f1726b1ac3457e3a91a05"
            }
        }

def serialize_adm2(doc):
    return {
        "id": str(doc.id),
        "ext_id": doc.ext_id,
        "name": doc.name,
        "adm1_id": str(doc.adm1_id.id) if doc.adm1_id else None,
        "adm1_name": str(doc.adm1_id.name) if doc.adm1_id else None
    }

@router.get("/", response_model=List[Adm2Schema])
def get_all_adm2():
    """
    Get all Adm2 records.
    """
    all_adm = Adm2.objects().limit(1000)
    return [serialize_adm2(adm) for adm in all_adm]

@router.get("/by-ids", response_model=List[Adm2Schema])
def get_adm2_by_ids(
    ids: str = Query(..., description="Comma-separated list of IDs. Example: ?ids=665f1726b1ac3457e3a91a05,665f1726b1ac3457e3a91a06")
):
    """
    Retrieve one or multiple adm2 records by their MongoDB ObjectIds.
    """
    search_ids = parse_object_ids(ids)
    matches = Adm2.objects(id__in=search_ids)
    return [serialize_adm2(adm) for adm in matches]

@router.get("/by-name", response_model=List[Adm2Schema])
def get_adm2_by_name(
    name: str = Query(..., description="One or more names (comma-separated) to match partially and case-insensitive")
):
    """
    Get Adm2 records that partially match one or more names.
    Example: /adm2/by-name?name=Cali,Palmira
    """
    terms = [term.strip() for term in name.split(",") if term.strip()]
    query = build_search_query(terms, ["name"])
    matches = Adm2.objects(__raw__=query)
    return [serialize_adm2(adm) for adm in matches]

@router.get("/by-adm1", response_model=List[Adm2Schema])
def get_adm2_by_adm1_ids(
    ids: str = Query(..., description="Comma-separated Adm1 IDs to filter Adm2 records")
):
    """
    Retrieve Adm2 records that belong to one or more Adm1 IDs.
    Example: /adm2/by-adm1?ids=665f1726b1ac3457e3a91a05,665f1726b1ac3457e3a91a06
    """
    search_ids = parse_object_ids(ids)
    matches = Adm2.objects(adm1_id__in=search_ids)
    return [serialize_adm2(adm) for adm in matches]

@router.get("/paged/", response_model=PaginatedResponse[Adm2Schema])
def get_adm2_paginated(
    page: int = Query(1, ge=1, description="Page number to retrieve. Ignored if 'skip' is defined"),
    limit: int = Query(10, ge=1, description="Maximum records per page"),
    skip: Optional[int] = Query(None, ge=0, description="Number of records to skip. If defined, overrides 'page' parameter"),
    search: Optional[str] = Query(None, description="Comma-separated search terms for partial, case-insensitive match"),
    search_fields: Optional[str] = Query(None, description="Comma-separated list of fields to search (e.g., name,ext_id)"),
    order_by: Optional[str] = Query(None, description="Comma-separated fields to sort by. Use '-' for descending (e.g., name,-ext_id)")
):
    """Retrieve paginated Adm2 records with optional search and sorting."""

    base_query = Adm2.objects
    allowed_fields = {"name", "ext_id"}
    # Validate and parse search fields
    if search_fields:
        fields = [f.strip() for f in search_fields.split(",") if f.strip()]
        invalid_fields = [f for f in fields if f not in allowed_fields]
        if invalid_fields:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid search fields: {', '.join(invalid_fields)}. Valid options: {', '.join(allowed_fields)}"
            )
    else:
        fields = list(allowed_fields)

    # Validate and parse sort fields
    sort_fields = []
    invalid_fields = []

    if order_by:
        for f in order_by.split(","):
            field = f.strip()
            field_clean = field.replace("-", "")
            if field_clean in allowed_fields:
                sort_fields.append(field)
            else:
                invalid_fields.append(field)

    if invalid_fields:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid sort fields: {', '.join(invalid_fields)}. Valid options: {', '.join(allowed_fields)}"
        )

    # Apply search filter if provided
    if search and fields:
        terms = [t.strip() for t in search.split(",") if t.strip()]
        base_query = base_query(__raw__=build_search_query(terms, fields))

    return build_paginated_response(
        base_query=base_query,
        schema_model=Adm2Schema,
        page=page,
        limit=limit,
        skip=skip,
        order_by_fields=sort_fields,
        serialize_fn=serialize_adm2
    )
