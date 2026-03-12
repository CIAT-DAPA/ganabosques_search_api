from fastapi import Depends, APIRouter
from typing import Optional, List
from pydantic import BaseModel, Field, ConfigDict
from ganabosques_orm.collections.adm3risk import Adm3Risk

from routes.base_route import generate_read_only_router
from dependencies.auth_guard import require_admin


class Adm3RiskSchema(BaseModel):
    id: str = Field(..., description="MongoDB internal ID of the risk record")
    adm3_id: Optional[str] = Field(None, description="ID of the associated Adm3 document")
    analysis_id: Optional[str] = Field(None, description="ID of the associated Analysis document")
    def_ha: Optional[float] = Field(None, description="Deforestation area in hectares")
    farm_amount: Optional[int] = Field(None, description="Number of farms in the area")
    risk_total: Optional[float] = Field(None, description="Total calculated risk index")

    model_config = ConfigDict(
        from_attributes=True,
        json_schema_extra={
            "example": {
                "id": "6661aaaae2ac3457e3a92abc",
                "adm3_id": "665f9999b1ac3457e3a91d01",
                "analysis_id": "6660aaaab1ac3457e3a91f88",
                "def_ha": 32.75,
                "farm_amount": 15,
                "risk_total": 7.95
            }
        }
    )


_inner_router = generate_read_only_router(
    prefix="/adm3risk",
    tags=["Analysis risk"],
    collection=Adm3Risk,
    schema_model=Adm3RiskSchema,
    allowed_fields=[],
    serialize_fn=None,  # convert_doc_to_json + as_pymongo() via base_route
    include_endpoints=["paged"]
)

router = APIRouter(
    dependencies=[Depends(require_admin)]
)

router.include_router(_inner_router)