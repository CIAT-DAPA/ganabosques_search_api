import re
import time
from fastapi import Query, HTTPException, Depends, APIRouter
from typing import Optional, List
from pydantic import BaseModel, Field, ConfigDict
from bson import ObjectId
from ganabosques_orm.collections.farm import Farm
from tools.pagination import build_paginated_response, PaginatedResponse
from schemas.logschema import LogSchema
from schemas.extid_schema import ExtIdFarmSchema

from routes.base_route import generate_read_only_router
from tools.utils import parse_object_ids, build_search_query, convert_doc_to_json

from datetime import datetime
from ganabosques_orm.enums.source import Source
from ganabosques_orm.enums.farmsource import FarmSource

from dependencies.auth_guard import require_admin  


class FarmSchema(BaseModel):
    """Optimized read-only schema for Farm collection."""
    id: str = Field(..., description="Internal MongoDB ID of the farm")
    adm3_id: str = Field(..., description="ID of the associated Adm3 document")
    ext_id: List[ExtIdFarmSchema] = Field(default_factory=list, description="List of external identifiers")
    farm_source: FarmSource = Field(..., description="Source from which the farm was registered")
    log: Optional[LogSchema] = Field(None, description="Logging information")

    model_config = ConfigDict(
        from_attributes=True,
        use_enum_values=True,
        json_schema_extra={
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
    )


# Router interno generado (sin auth) - Pydantic optimizado sin serialize_fn
_inner_router = generate_read_only_router(
    prefix="/farm",
    tags=["Farm and Enterprise"],
    collection=Farm,
    schema_model=FarmSchema,
    allowed_fields=["farm_source"],
    serialize_fn=None,  # Pydantic lo hace automáticamente
    include_endpoints=["paged", "by-extid"],
    include_get_all=False  # Desactivar el endpoint automático para usar uno optimizado
)


@_inner_router.get("/", response_model=List[FarmSchema])
def get_all_farms_optimized():
    """
    Retrieve all Farm records.
    WARNING: This endpoint returns all farms. Use /paged/ endpoint for better performance.
    """
    try:
        inicio_query = time.perf_counter()
        docs = list(Farm.objects().as_pymongo())
        fin_query = time.perf_counter()
        
        inicio_serialization = time.perf_counter()
        items = [convert_doc_to_json(doc) for doc in docs]
        fin_serialization = time.perf_counter()
        
        query_time = fin_query - inicio_query
        serialization_time = fin_serialization - inicio_serialization
        
        print(f"[Farm GET /] Query time: {query_time:.3f}s | Serialization time: {serialization_time:.3f}s | Total: {(query_time + serialization_time):.3f}s | Records: {len(items)}")
        
        return items
    except Exception as e:
        print(f"[Farm GET /] ERROR: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Error retrieving all farms: {str(e)}"
        )


@_inner_router.get("/by-adm3", response_model=List[FarmSchema])
def get_farm_by_adm3_ids(
    ids: str = Query(..., description="Comma-separated Adm3 IDs to filter Farms records")
):
    """
    Retrieve Farm records that belong to one or more Adm3 IDs.
    Optimized with as_pymongo() for better performance.
    Example: /by-adm3?ids=665f1726b1ac3457e3a91a05,665f1726b1ac3457e3a91a06
    """
    try:
        search_ids = parse_object_ids(ids)
        
        inicio = time.perf_counter()
        docs = list(Farm.objects(adm3_id__in=search_ids).as_pymongo())
        items = [convert_doc_to_json(doc) for doc in docs]
        fin = time.perf_counter()
        
        print(f"[Farm /by-adm3] Time: {(fin - inicio):.3f}s | Records: {len(items)}")
        
        return items
    except HTTPException:
        raise
    except Exception as e:
        print(f"[Farm /by-adm3] ERROR: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Error retrieving farms by adm3: {str(e)}"
        )


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
#
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
#
#     ext_conditions = []
#
#     if ext_codes:
#         ext_code_list = [re.escape(code.strip()) for code in ext_codes.split(",") if code.strip()]
#         ext_conditions.append({"ext_id": {"$elemMatch": {"ext_code": {"$in": ext_code_list }}}})
#
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
#
#     query = {"$and": ext_conditions} if len(ext_conditions) == 2 else ext_conditions[0]
#     farms = Farm.objects(__raw__=query)
#     return [serialize_farm(farm) for farm in farms]


router = APIRouter(
    dependencies=[Depends(require_admin)]  
)

router.include_router(_inner_router)