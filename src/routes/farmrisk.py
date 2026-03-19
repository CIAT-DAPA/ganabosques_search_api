import time
from typing import Optional, List
from fastapi import Depends, APIRouter, Query, HTTPException
from pydantic import BaseModel, Field
from ganabosques_orm.collections.farmrisk import FarmRisk
from routes.base_route import generate_read_only_router
from dependencies.auth_guard import require_admin
from tools.utils import parse_object_ids, convert_doc_to_json


class AttributeSchema(BaseModel):
    prop: Optional[float] = Field(None, description="Proportion")
    ha: Optional[float] = Field(None, description="Area in hectares")
    distance: Optional[float] = Field(None, description="Distance to feature")


class FarmRiskSchema(BaseModel):
    id: str = Field(..., description="MongoDB internal ID of the farm risk")
    farm_id: Optional[str] = Field(None, description="ID of the associated farm")
    analysis_id: Optional[str] = Field(None, description="ID of the associated analysis")
    farm_polygons_id: Optional[str] = Field(None, description="ID of the associated farm polygon")

    deforestation: Optional[AttributeSchema] = Field(None, description="Deforestation attributes")
    protected: Optional[AttributeSchema] = Field(None, description="Protected area attributes")
    farming_in: Optional[AttributeSchema] = Field(None, description="Attributes for farming_in")
    farming_out: Optional[AttributeSchema] = Field(None, description="Attributes for farming_out")

    risk_direct: Optional[bool] = Field(None, description="Direct risk (>0 -> true, else false)")
    risk_input: Optional[bool] = Field(None, description="Input-based risk (>0 -> true, else false)")
    risk_output: Optional[bool] = Field(None, description="Output-based risk (>0 -> true, else false)")

    class Config:
        from_attributes = True
        json_schema_extra = {
            "example": {
                "id": "68a36b0c12967887c405a8dc",
                "farm_id": "689a803713d2089e5c8b1e2e",
                "analysis_id": "689d1149a6ef4a011d5c394e",
                "farm_polygons_id": "689a803713d2089e5c8b1e2f",
                "deforestation": { "prop": 0, "ha": 0, "distance": 0 },
                "protected": { "prop": 1, "ha": 4.675431693, "distance": 0 },
                "farming_in": { "prop": 0.32, "ha": 1.234, "distance": 12 },
                "farming_out": { "prop": 0.05, "ha": 0.421, "distance": 3 },
                "risk_direct": True,
                "risk_input": False,
                "risk_output": False
            }
        }


def _as_str_oid(value) -> Optional[str]:
    if not value:
        return None
    return str(getattr(value, "id", value))


def _serialize_attr(attr) -> Optional[dict]:
    if not attr:
        return None
    return {
        "prop": getattr(attr, "prop", None),
        "ha": getattr(attr, "ha", None),
        "distance": getattr(attr, "distance", None),
    }


def serialize_farmrisk(doc):
    return {
        "id": str(doc.id),
        "farm_id": _as_str_oid(getattr(doc, "farm_id", None)),
        "analysis_id": _as_str_oid(getattr(doc, "analysis_id", None)),
        "farm_polygons_id": _as_str_oid(getattr(doc, "farm_polygons_id", None)),
        "deforestation": _serialize_attr(getattr(doc, "deforestation", None)),
        "protected": _serialize_attr(getattr(doc, "protected", None)),
        "farming_in": _serialize_attr(getattr(doc, "farming_in", None)),
        "farming_out": _serialize_attr(getattr(doc, "farming_out", None)),
        "risk_direct": getattr(doc, "risk_direct", None),
        "risk_input": getattr(doc, "risk_input", None),
        "risk_output": getattr(doc, "risk_output", None),
    }


_inner_router = generate_read_only_router(
    prefix="/farmrisk",
    tags=["Analysis risk"],
    collection=FarmRisk,
    schema_model=FarmRiskSchema,
    allowed_fields=[
        "farm_id", "analysis_id",
        "risk_direct", "risk_input", "risk_output",
    ],
    serialize_fn=serialize_farmrisk,
    include_endpoints=["paged"]
)


@_inner_router.get("/by-analysis", response_model=List[FarmRiskSchema])
def get_farmrisk_by_analysis_ids(
    ids: str = Query(..., description="Comma-separated Analysis IDs to filter FarmRisk records")
):
    """
    Retrieve FarmRisk records that belong to one or more Analysis IDs.
    Example: /by-analysis?ids=665f1726b1ac3457e3a91a05,665f1726b1ac3457e3a91a06
    """
    try:
        search_ids = parse_object_ids(ids)

        inicio = time.perf_counter()
        docs = list(FarmRisk.objects(analysis_id__in=search_ids).as_pymongo())
        items = [convert_doc_to_json(doc) for doc in docs]
        fin = time.perf_counter()

        print(f"[FarmRisk /by-analysis] Time: {(fin - inicio):.3f}s | Records: {len(items)}")

        return items
    except HTTPException:
        raise
    except Exception as e:
        print(f"[FarmRisk /by-analysis] ERROR: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Error retrieving farmrisk by analysis ids: {str(e)}"
        )

router = APIRouter(
    dependencies=[Depends(require_admin)]
)

router.include_router(_inner_router)