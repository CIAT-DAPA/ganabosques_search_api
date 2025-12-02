import re
from fastapi import Query, HTTPException, Depends, APIRouter
from typing import Optional, List
from pydantic import BaseModel, Field
from bson import ObjectId
from ganabosques_orm.collections.adm3risk import Adm3Risk
from tools.pagination import build_paginated_response, PaginatedResponse

from routes.base_route import generate_read_only_router
from tools.utils import parse_object_ids, build_search_query
from dependencies.auth_guard import require_admin


class Adm3RiskSchema(BaseModel):
    id: str = Field(..., description="MongoDB internal ID of the risk record")
    adm3_id: Optional[str] = Field(None, description="ID of the associated Adm3 document")
    analysis_id: Optional[str] = Field(None, description="ID of the associated Analysis document")
    def_ha: Optional[float] = Field(None, description="Deforestation area in hectares")
    farm_amount: Optional[int] = Field(None, description="Number of farms in the area")
    risk_total: Optional[float] = Field(None, description="Total calculated risk index")

    class Config:
        from_attributes = True
        json_schema_extra = {
            "example": {
                "id": "6661aaaae2ac3457e3a92abc",
                "adm3_id": "665f9999b1ac3457e3a91d01",
                "analysis_id": "6660aaaab1ac3457e3a91f88",
                "def_ha": 32.75,
                "farm_amount": 15,
                "risk_total": 7.95
            }
        }


def serialize_adm3risk(doc):
    return {
        "id": str(doc.id),
        "adm3_id": str(doc.adm3_id.id) if doc.adm3_id else None,
        "analysis_id": str(doc.analysis_id.id) if doc.analysis_id else None,
        "def_ha": doc.def_ha,
        "farm_amount": doc.farm_amount,
        "risk_total": doc.risk_total
    }


_inner_router = generate_read_only_router(
    prefix="/adm3risk",
    tags=["Analysis risk"],
    collection=Adm3Risk,
    schema_model=Adm3RiskSchema,
    allowed_fields=[],
    serialize_fn=serialize_adm3risk,
    include_endpoints=["paged"]
)

router = APIRouter(
    dependencies=[Depends(require_admin)]
)

router.include_router(_inner_router)