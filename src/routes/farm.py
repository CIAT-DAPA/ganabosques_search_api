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
    include_endpoints=["paged", "by-extid"]
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

# Lista de opciones válidas del enum Source
valid_sources_str = ", ".join([s.value for s in Source])

# @router.get("/by-extid", response_model=List[FarmSchema])
# def get_farm_by_extid(
#     ext_codes: Optional[str] = Query(
#         None,
#         description="Comma-separated list of ext_code values to search in ext_id.ext_code"
#     ),
#     sources: Optional[str] = Query(
#         None,
#         description=f"Comma-separated list of source values to search in ext_id.source. Valid options: {valid_sources_str}"
#     )
# ):
#     """
#     Retrieve Farm records that match one or more ext_id.ext_code or ext_id.source values.
#     At least one of the two parameters must be provided.

#     Examples:
#     - /farm/by-ext-id?ext_codes=SIT-2022-0001,SIT-2023-0005
#     - /farm/by-ext-id?sources=SIT_CODE,SAGARI
#     - /farm/by-ext-id?ext_codes=SIT-2022-0001&sources=SIT_CODE
#     """
#     if not ext_codes and not sources:
#         raise HTTPException(
#             status_code=400,
#             detail="At least one of 'ext_codes' or 'sources' must be provided."
#         )

#     ext_conditions = []

#     if ext_codes:
#         ext_code_list = [re.escape(code.strip()) for code in ext_codes.split(",") if code.strip()]
#         ext_conditions.append({"ext_id": {"$elemMatch": {"ext_code": {"$in": ext_code_list }}}})

#     if sources:
#         raw_sources = [s.strip() for s in sources.split(",") if s.strip()]
#         invalid_sources = [s for s in raw_sources if s not in Source.__members__]
#         if invalid_sources:
#             raise HTTPException(
#                 status_code=400,
#                 detail=f"Invalid source(s): {', '.join(invalid_sources)}. "
#                        f"Valid options: {valid_sources_str}"
#             )
#         enum_sources = [re.escape(Source[s].value) for s in raw_sources]
#         ext_conditions.append({
#             "ext_id": {"$elemMatch": {"source": {"$in": enum_sources }}}})

#     query = {"$and": ext_conditions} if len(ext_conditions) == 2 else ext_conditions[0]
#     farms = Farm.objects(__raw__=query)
#     return [serialize_farm(farm) for farm in farms]