import re
from fastapi import APIRouter, Query, HTTPException
from typing import Optional, List
from pydantic import BaseModel, Field
from bson import ObjectId
from ganabosques_orm.collections.adm1 import Adm1
from tools.pagination import build_paginated_response, PaginatedResponse
from tools.utils import parse_object_ids, build_search_query

router = APIRouter(
    prefix="/adm1",
    tags=["Admin levels"]
)

class Adm1Schema(BaseModel):
    id: str = Field(..., description="Internal MongoDB ID")
    ext_id: Optional[str] = Field(None, description="External administrative region ID")
    name: Optional[str] = Field(None, description="Name of the administrative region")
    ugg_size: Optional[float] = Field(None, description="UGG size associated with the region")

    class Config:
        from_attributes = True
        json_schema_extra = {
            "example": {
                "id": "665f1726b1ac3457e3a91a05",
                "ext_id": "5",
                "name": "ANTIOQUIA",
                "ugg_size": 1.7
            }
        }

def serialize_adm1(adm):
    """Serialize a MongoEngine Adm1 document into a JSON-compatible dict."""
    return {
        "id": str(adm.id),
        "ext_id": adm.ext_id,
        "name": adm.name,
        "ugg_size": adm.ugg_size
    }

@router.get("/", response_model=List[Adm1Schema])
def get_all_adm1():
    """Retrieve all Adm1 records."""
    all_adm1 = Adm1.objects()
    return [serialize_adm1(adm) for adm in all_adm1]

@router.get("/by-ids", response_model=List[Adm1Schema])
def get_adm1_by_ids(
    ids: str = Query(..., description="Comma-separated list of IDs. Example: ?ids=665f1726b1ac3457e3a91a05,665f1726b1ac3457e3a91a06")
):
    """Retrieve multiple Adm1 records by their MongoDB IDs."""
    search_ids = parse_object_ids(ids)
    matches = Adm1.objects(id__in=search_ids)
    return [serialize_adm1(adm) for adm in matches]

@router.get("/by-name", response_model=List[Adm1Schema])
def get_adm1_by_name(
    name: str = Query(..., description="One or more comma-separated names for case-insensitive partial search")
):
    """Search Adm1 records by name with partial, case-insensitive match."""
    terms = [term.strip() for term in name.split(",") if term.strip()]
    query = build_search_query(terms, ["name"])
    matches = Adm1.objects(__raw__=query)
    return [serialize_adm1(adm) for adm in matches]

@router.get("/by-extid", response_model=List[Adm1Schema])
def get_adm1_by_extid(
    ext_ids: str = Query(..., description="One or more comma-separated ext_id for case-insensitive partial search")
):
    """Search Adm1 records by ext_id with partial, case-insensitive match."""
    terms = [term.strip() for term in ext_ids.split(",") if term.strip()]
    query = build_search_query(terms, ["ext_id"])
    matches = Adm1.objects(__raw__=query)
    return [serialize_adm1(adm) for adm in matches]

@router.get("/paged/", response_model=PaginatedResponse[Adm1Schema])
def get_adm1_paginated(
    page: int = Query(1, ge=1, description="Page number to retrieve. Ignored if 'skip' is defined"),
    limit: int = Query(10, ge=1, description="Maximum records per page"),
    skip: Optional[int] = Query(None, ge=0, description="Number of records to skip. If defined, overrides 'page' parameter"),
    search: Optional[str] = Query(None, description="Comma-separated search terms for partial, case-insensitive match"),
    search_fields: Optional[str] = Query(None, description="Comma-separated list of fields to search (e.g., name,ext_id)"),
    order_by: Optional[str] = Query(None, description="Comma-separated fields to sort by. Use '-' for descending (e.g., name,-ext_id)")
):
    """Retrieve paginated Adm1 records with optional search and sorting."""
    base_query = Adm1.objects
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

    # Validate and parse sort field
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
        terms = [term.strip() for term in search.split(",") if term.strip()]
        base_query = base_query(__raw__=build_search_query(terms, fields))

    return build_paginated_response(
        base_query=base_query,
        schema_model=Adm1Schema,
        page=page,
        limit=limit,
        skip=skip,
        order_by_fields=sort_fields,
    )
