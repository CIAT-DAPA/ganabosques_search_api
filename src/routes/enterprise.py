import re
import time
from fastapi import Query, HTTPException, Depends, APIRouter
from typing import Optional, List
from pydantic import BaseModel, Field, ConfigDict
from bson import ObjectId
from ganabosques_orm.collections.enterprise import Enterprise
from tools.pagination import build_paginated_response, PaginatedResponse
from schemas.logschema import LogSchema
from schemas.extid_schema import ExtIdEnterpriseSchema
from tools.utils import convert_doc_to_json, parse_object_ids

from routes.base_route import generate_read_only_router

from datetime import datetime
from ganabosques_orm.enums.typeenterprise import TypeEnterprise
from ganabosques_orm.enums.label import Label

from dependencies.auth_guard import require_admin


class EnterpriseSchema(BaseModel):
    """Optimized read-only schema for Enterprise collection."""
    id: str = Field(..., description="Internal MongoDB ID of the enterprise")
    adm2_id: Optional[str] = Field(None, description="ID of the associated Adm2 document")
    name: Optional[str] = Field(None, description="Name of the enterprise")
    ext_id: List[ExtIdEnterpriseSchema] = Field(default_factory=list, description="List of external identifiers")
    type_enterprise: TypeEnterprise = Field(..., description="Type of the enterprise (e.g., SLAUGHTERHOUSE, COLLECTION_CENTER)")
    latitude: Optional[float] = Field(None, description="Latitude of the enterprise location")
    longitud: Optional[float] = Field(None, description="Longitude of the enterprise location")
    log: Optional[LogSchema] = Field(None, description="Logging metadata")
    
    model_config = ConfigDict(
        from_attributes=True,
        use_enum_values=True,
        json_schema_extra={
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
    )


# Router interno generado (sin auth) - Pydantic optimizado sin serialize_fn
_inner_router = generate_read_only_router(
    prefix="/enterprise",
    tags=["Farm and Enterprise"],
    collection=Enterprise,
    schema_model=EnterpriseSchema,
    allowed_fields=["name", "type_enterprise"],
    serialize_fn=None,  # Pydantic lo hace automáticamente
    include_endpoints=["paged", "by-name", "by-extid"]
)


@_inner_router.get("/by-adm2", response_model=List[EnterpriseSchema])
def get_enterprise_by_adm2_ids(
    ids: str = Query(..., description="Comma-separated Adm2 IDs to filter Enterprises records")
):
    """
    Retrieve Enterprise records that belong to one or more Adm2 IDs.
    Optimized with as_pymongo() for better performance.
    Example: /by-adm2?ids=665f1726b1ac3457e3a91a05,665f1726b1ac3457e3a91a06
    """
    try:
        search_ids = parse_object_ids(ids)
        
        inicio = time.perf_counter()
        docs = list(Enterprise.objects(adm2_id__in=search_ids).as_pymongo())
        items = [convert_doc_to_json(doc) for doc in docs]
        fin = time.perf_counter()
        
        print(f"[Enterprise /by-adm2] Time: {(fin - inicio):.3f}s | Records: {len(items)}")
        
        return items
    except HTTPException:
        raise
    except Exception as e:
        print(f"[Enterprise /by-adm2] ERROR: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Error retrieving enterprises by adm2: {str(e)}"
        )


# valid_labels_str = ", ".join([l.name for l in Label])

# @router.get("/by-extid", response_model=List[EnterpriseSchema])
# def get_enterprise_by_extid(
#     ext_codes: Optional[str] = Query(
#         None,
#         description="Comma-separated ext_code values to filter in ext_id.ext_code"
#     ),
#     labels: Optional[str] = Query(
#         None,
#         description=f"Comma-separated label values to filter in ext_id.label. Valid options: {valid_labels_str}"
#     )
# ):
#     ...
#     (NO TOCADO)





router = APIRouter(
    dependencies=[Depends(require_admin)] 
)

router.include_router(_inner_router)